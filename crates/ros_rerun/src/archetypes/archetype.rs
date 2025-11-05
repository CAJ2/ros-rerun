use std::fmt::Debug;

use ahash::{HashMap, HashMapExt as _};
use anyhow::Result;
use async_trait::async_trait;
use dyn_clone::DynClone;
use log::debug;
use rclrs::DynamicMessageError;
use rerun::external::re_types_core::ArchetypeName;
use thiserror::Error;

use crate::{
    archetypes::{text::TextDocument, ROSTypeName, ROSTypeString},
    channel::LogData,
    config::defs::ConverterSettings,
};

#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("Unable to convert from ROS type {} to archetype {name}", ros_type.as_deref().unwrap_or("<ANY>"))]
    UnsupportedConversion {
        name: ArchetypeName,
        ros_type: Option<String>,
    },

    #[error("Unable to parse conversion config for archetype {0} and ROS type {1}")]
    ConfigParseError(ArchetypeName, String),

    #[error("Conversion error for archetype {0} and ROS type {1}: {2}")]
    ConversionError(ArchetypeName, String, anyhow::Error),
}

/// Trait for configuring an archetype converter.
///
/// All converters must implement this trait.
/// Using a pub(super) trait prevents any configuration from changing
/// outside this module after the converter has been built.
pub(super) trait ConverterConfigurable: ArchetypeConverter {
    /// Set the topic for the converter.
    fn set_topic(&mut self, topic: &str);

    /// Set the ROS message type for the converter.
    fn set_ros_type(&mut self, ros_type: Option<ROSTypeName>);

    /// Set the configuration for the converter.
    ///
    /// # Errors
    /// Returns `ConfigParseError` if the configuration is invalid.
    fn set_config(&mut self, config: ConverterSettings) -> Result<(), ConverterError>;
}

dyn_clone::clone_trait_object!(ConverterConfigurable);

/// Trait for converting ROS messages into Rerun archetypes.
#[async_trait]
pub trait ArchetypeConverter: DynClone + Send + Sync {
    /// Get the name of the Rerun archetype.
    fn rerun_name(&self) -> ArchetypeName;

    /// Get the ROS message types that this converter can process.
    fn ros_types(&self) -> Option<Vec<ROSTypeString<'static>>> {
        None
    }

    fn supports_custom(&self) -> bool {
        false
    }

    /// Convert a ROS message to a Rerun archetype.
    ///
    /// Each instance of a converter needs to store the ROS topic and type information.
    /// This means `set_config` must be called before `convert`.
    async fn convert<'a>(
        &self,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> Result<LogData, ConverterError>;
}

dyn_clone::clone_trait_object!(ArchetypeConverter);

/// Builder for configuring archetype converters.
///
/// It abstracts over finding the correct converter from the registry
/// and setting up the trait object.
pub struct ConverterBuilder<'a> {
    registry: &'a ConverterRegistry,
    topic: String,
    ros_type: Option<ROSTypeName>,
    archetype: Option<ArchetypeName>,
    config: Option<ConverterSettings>,
}

impl<'a> ConverterBuilder<'a> {
    pub fn new_with_registry(registry: &'a ConverterRegistry) -> Self {
        Self {
            registry,
            topic: String::new(),
            archetype: None,
            ros_type: None,
            config: None,
        }
    }

    pub fn topic(mut self, topic: &str) -> Self {
        self.topic = topic.to_owned();
        self
    }

    pub fn ros_type(mut self, ros_type: ROSTypeName) -> Self {
        self.ros_type = Some(ros_type);
        self
    }

    pub fn archetype(mut self, archetype: ArchetypeName) -> Self {
        self.archetype = Some(archetype);
        self
    }

    pub fn config(mut self, config: ConverterSettings) -> Self {
        self.config = Some(config);
        self
    }

    /// Builds the converter.
    ///
    /// # Errors
    /// Returns `ConverterError::UnsupportedConversion` if no suitable converter is found.
    pub fn build(self) -> Result<Box<dyn ArchetypeConverter>, ConverterError> {
        let converter = self
            .registry
            .find_converter(self.ros_type.as_ref(), self.archetype)?;
        let mut converter: Box<dyn ConverterConfigurable> = match converter {
            FoundConverter::ArchetypeCustom(c) | FoundConverter::ArchetypeROSType(c) => c,
        };
        converter.set_topic(&self.topic);
        converter.set_ros_type(self.ros_type);
        if let Some(config) = self.config {
            converter.set_config(config)?;
        }
        Ok(converter)
    }
}

/// Registry for archetype converters.
///
/// A converter can register two types of conversions:
/// - Convert a specific ROS message type to a Rerun archetype
/// - Try to convert any ROS message type to a Rerun archetype
///
/// When topic subscribers are built, they will prefer specific
/// ROS message type converters over general converters.
///
/// The registry also supports validating the settings defined
/// by each `ArchetypeConverter`.
pub struct ConverterRegistry {
    /// All registered converters keyed by the archetype and optionally the ROS type
    /// If the converter supports a general conversion, it will be registered with (`ArchetypeName`, None)
    converters: HashMap<(ArchetypeName, Option<ROSTypeName>), Box<dyn ConverterConfigurable>>,
    /// Tracks converters by ROS type when the archetype needs to be inferred
    /// This essentially defines the default archetype for a given ROS type
    converters_by_ros_type: HashMap<ROSTypeName, Box<dyn ConverterConfigurable>>,
    /// Tracks errors for ROS type definitions that could not be found in the current environment
    error_types: HashMap<(ArchetypeName, String), Vec<DynamicMessageError>>,
}

impl ConverterRegistry {
    pub fn init() -> Self {
        let mut registry = Self {
            converters: HashMap::new(),
            converters_by_ros_type: HashMap::new(),
            error_types: HashMap::new(),
        };

        // All archetype converters are registered here
        registry.register(&TextDocument::default());

        registry
    }

    /// Find a converter for a ROS type and optionally an archetype.
    /// If the archetype is not specified, it will pick the default converter for the ROS type, if any.
    ///
    /// # Errors
    /// Returns `ConverterError::UnsupportedConversion` if no suitable converter is found.
    fn find_converter(
        &self,
        ros_type: Option<&ROSTypeName>,
        archetype_name: Option<ArchetypeName>,
    ) -> FindConverterResult {
        match (ros_type, archetype_name) {
            (Some(ros_type), Some(name)) => self.find_converter_for_archetype(name, ros_type),
            (Some(ros_type), None) => self.find_converter_for_ros_type(ros_type),
            (None, Some(name)) => self.find_converter_for_generic_archetype(name),
            (None, None) => Err(ConverterError::UnsupportedConversion {
                name: ArchetypeName::from("<ANY>"),
                ros_type: None,
            }),
        }
    }

    fn find_converter_for_archetype(
        &self,
        archetype_name: ArchetypeName,
        ros_type: &ROSTypeName,
    ) -> FindConverterResult {
        let archetype_name = fully_qualified_name(archetype_name);
        self.converters
            .get(&(archetype_name, Some(ros_type.clone())))
            .map(|converter| Ok(FoundConverter::ArchetypeROSType(converter.clone())))
            .or_else(|| {
                self.converters
                    .get(&(archetype_name, None))
                    .map(|converter| Ok(FoundConverter::ArchetypeCustom(converter.clone())))
            })
            .unwrap_or(Err(ConverterError::UnsupportedConversion {
                name: archetype_name,
                ros_type: Some(format!("{ros_type}")),
            }))
    }

    fn find_converter_for_generic_archetype(
        &self,
        archetype_name: ArchetypeName,
    ) -> FindConverterResult {
        let archetype_name = fully_qualified_name(archetype_name);
        self.converters
            .get(&(archetype_name, None))
            .map(|converter| Ok(FoundConverter::ArchetypeCustom(converter.clone())))
            .unwrap_or(Err(ConverterError::UnsupportedConversion {
                name: archetype_name,
                ros_type: None,
            }))
    }

    fn find_converter_for_ros_type(&self, ros_type: &ROSTypeName) -> FindConverterResult {
        if let Some(converter) = self.converters_by_ros_type.get(ros_type) {
            Ok(FoundConverter::ArchetypeROSType(converter.clone()))
        } else {
            Err(ConverterError::UnsupportedConversion {
                name: ArchetypeName::from("<ANY>"),
                ros_type: Some(format!("{ros_type}")),
            })
        }
    }

    fn register<T>(&mut self, converter: &T)
    where
        T: ConverterConfigurable + Clone + 'static,
    {
        let archetype_name = converter.rerun_name();
        if converter.supports_custom() {
            self.register_converter(
                archetype_name,
                None,
                Box::new(converter.clone()) as Box<dyn ConverterConfigurable>,
            );
        }
        let ros_types = converter.ros_types();
        if let Some(ros_types) = &ros_types {
            for ros_type in ros_types {
                self.register_converter(
                    archetype_name,
                    Some(ros_type),
                    Box::new(converter.clone()) as Box<dyn ConverterConfigurable>,
                );
            }
        }
    }

    // Register a conversion from an archetype converter.
    fn register_converter(
        &mut self,
        archetype_name: ArchetypeName,
        ros_type: Option<&ROSTypeString<'_>>,
        converter: Box<dyn ConverterConfigurable>,
    ) {
        let parsed_type = ros_type.map(ROSTypeName::try_from).transpose();
        match parsed_type {
            Ok(Some(ros_type)) => {
                debug!("Registered converter for {archetype_name} with ROS type {ros_type}");
                if !self.converters_by_ros_type.contains_key(&ros_type) {
                    self.converters_by_ros_type
                        .insert(ros_type.clone(), converter.clone());
                }
                self.converters
                    .insert((archetype_name, Some(ros_type)), converter);
            }
            Ok(None) => {
                debug!("Registered generic converter for {archetype_name}");
                self.converters.insert((archetype_name, None), converter);
            }
            Err(err) => {
                debug!(
                    "Failed to register converter for {archetype_name} with ROS type {ros_type:?}: {err}"
                );
                if let Some(ros_type) = ros_type {
                    self.error_types
                        .insert((archetype_name, format!("{ros_type}")), vec![err]);
                }
            }
        };
    }
}

pub(super) enum FoundConverter {
    // Converter for a specific ROS message type to a Rerun archetype
    ArchetypeROSType(Box<dyn ConverterConfigurable>),
    // Converter for any ROS message type to a Rerun archetype
    ArchetypeCustom(Box<dyn ConverterConfigurable>),
    // TODO: Can/should we support always converting ROS message
    // data even if it doesn't fully fit to Rerun components?
}

pub(super) type FindConverterResult = Result<FoundConverter, ConverterError>;

fn fully_qualified_name(name: ArchetypeName) -> ArchetypeName {
    if name.starts_with("rerun.archetypes.") {
        ArchetypeName::from(name.as_str())
    } else {
        ArchetypeName::new(format!("rerun.archetypes.{name}").as_str())
    }
}
