use ahash::{HashMap, HashMapExt as _};
use anyhow::Result;
use async_trait::async_trait;
use dyn_clone::DynClone;
use log::debug;
use rclrs::DynamicMessageError;
use rerun::external::re_types_core::ArchetypeName;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

use crate::{register::register_converters, ROSTypeName, ROSTypeString, RerunName};

#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("unable to convert from ROS type {} to archetype {name}", ros_type.as_deref().unwrap_or("<ANY>"))]
    UnsupportedConversion {
        name: RerunName,
        ros_type: Option<String>,
    },

    #[error("invalid conversion config for archetype {0} and ROS type {1}: {2}")]
    InvalidConfig(RerunName, String, anyhow::Error),

    #[error("failed to deserialize ROS message")]
    Deserialization(#[source] rclrs::dynamic_message::DynamicMessageError),

    #[error("conversion error for {0} from ROS type {1}: {2}")]
    Conversion(RerunName, String, anyhow::Error),
}

/// Trait for configuring a message converter.
///
/// All converters must implement this trait.
/// Using a pub(super) trait prevents any configuration from changing
/// outside this module after the converter has been built.
pub(super) trait ConverterCfg: Converter {
    /// Set the configuration for the converter.
    ///
    /// # Errors
    /// Returns `ConfigParseError` if the configuration is invalid.
    fn set_config(&mut self, config: ConverterSettings) -> Result<(), ConverterError>;
}

dyn_clone::clone_trait_object!(ConverterCfg);

/// Header information for messages
///
/// Maps to the ROS `std_msgs/Header` definition
/// and used to set the logged timepoint and
/// the coordinate frame of reference.
pub struct Header {
    pub time: rerun::TimeCell,
    pub frame_id: Option<String>,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            time: rerun::TimeCell::timestamp_now(),
            frame_id: None,
        }
    }
}

impl From<crate::definitions::std_msgs::Header> for Header {
    fn from(header: crate::definitions::std_msgs::Header) -> Self {
        let time = if header.stamp.sec == 0 && header.stamp.nanosec == 0 {
            rerun::TimeCell::timestamp_now()
        } else {
            rerun::TimeCell::from_timestamp_nanos_since_epoch(
                (header.stamp.sec as i64) * 1_000_000_000 + (header.stamp.nanosec as i64),
            )
        };
        Self {
            time,
            frame_id: if header.frame_id.is_empty() {
                None
            } else {
                Some(header.frame_id)
            },
        }
    }
}

#[derive(Clone)]
pub struct LogPacket {
    components: Arc<dyn rerun::AsComponents + Send + Sync>,
    header: Option<Arc<Header>>,
}

impl LogPacket {
    pub fn new(components: impl rerun::AsComponents + Send + Sync + 'static) -> Self {
        Self {
            components: Arc::new(components),
            header: None,
        }
    }

    pub fn with_header(mut self, header: impl Into<Arc<Header>>) -> Self {
        self.header = Some(header.into());
        self
    }

    pub fn as_serialized_batches(&self) -> Vec<rerun::SerializedComponentBatch> {
        self.components.as_serialized_batches()
    }
}

/// Trait for converting ROS messages into Rerun archetypes/components.
#[async_trait]
pub trait Converter: DynClone + Send + Sync {
    /// Get the name of the Rerun archetype.
    fn rerun_name(&self) -> RerunName;

    /// Get the ROS message type for this converter.
    ///
    /// When `None`, the converter supports any ROS message type.
    fn ros_type(&self) -> Option<&ROSTypeString<'static>>;

    /// Convert a ROS message view.
    async fn convert_view<'a>(
        &self,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> Result<LogPacket, ConverterError>;
}

dyn_clone::clone_trait_object!(Converter);

#[derive(Deserialize, Serialize, Clone, Default, Debug, PartialEq)]
pub struct ConverterSettings(pub toml::Table);

/// Builder for configuring archetype converters.
///
/// It abstracts over finding the correct converter from the registry
/// and setting up the trait object.
pub struct ConverterBuilder<'a> {
    registry: &'a ConverterRegistry,
    topic: String,
    ros_type: Option<ROSTypeName>,
    rerun_name: Option<RerunName>,
    config: Option<ConverterSettings>,
}

impl<'a> ConverterBuilder<'a> {
    pub fn new_with_registry(registry: &'a ConverterRegistry) -> Self {
        Self {
            registry,
            topic: String::new(),
            ros_type: None,
            rerun_name: None,
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

    pub fn rerun_name(mut self, rerun_name: RerunName) -> Self {
        self.rerun_name = Some(rerun_name);
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
    pub fn build(self) -> Result<Box<dyn Converter>, ConverterError> {
        let mut converter = self
            .registry
            .find_converter(self.ros_type.as_ref(), self.rerun_name.as_ref())?;
        if let Some(config) = self.config {
            converter.set_config(config)?;
        }
        Ok(converter)
    }
}

/// Registry for message converters.
///
/// A converter registers a single ROS type to Rerun archetype/components mapping.
/// There is a default converter for each ROS type, to allow the configuration
/// to omit the archetype when the default is reasonable.
///
/// There are also generic converters that can convert any ROS message type
/// to a Rerun archetype, e.g. for pretty-printing text documents.
/// These converters will only be used when the archetype is explicitly specified.
pub struct ConverterRegistry {
    /// All registered converters keyed by the ROS type and the Rerun archetype name.
    ///
    /// If the converter supports a general conversion, it will be registered with (`ArchetypeName`, None).
    converters: HashMap<(ROSTypeName, RerunName), Box<dyn ConverterCfg>>,
    /// Tracks converters by ROS type when the archetype needs to be inferred.
    ///
    /// This essentially defines the default archetype for a given ROS type.
    converters_by_ros_type: HashMap<ROSTypeName, Box<dyn ConverterCfg>>,
    /// Tracks generic converters that can (attempt to) convert any ROS type to a Rerun archetype.
    generic_converters: HashMap<RerunName, Box<dyn ConverterCfg>>,
    /// Tracks errors for ROS type definitions that could not be found in the current environment.
    error_types: HashMap<String, DynamicMessageError>,
}

impl ConverterRegistry {
    pub fn init() -> Self {
        let mut registry = Self {
            converters: HashMap::new(),
            converters_by_ros_type: HashMap::new(),
            generic_converters: HashMap::new(),
            error_types: HashMap::new(),
        };

        register_converters(&mut registry);

        registry
    }

    /// Find a converter for a ROS type and a Rerun name.
    /// If the Rerun name is not specified, it will pick the default converter for the ROS type, if any.
    ///
    /// # Errors
    /// Returns `ConverterError::UnsupportedConversion` if no suitable converter is found.
    pub(crate) fn find_converter(
        &self,
        ros_type: Option<&ROSTypeName>,
        rerun_name: Option<&RerunName>,
    ) -> FindConverterResult {
        match (ros_type, rerun_name) {
            (Some(ros_type), Some(name)) => self.find_converter_both(ros_type, name),
            (Some(ros_type), None) => self.find_converter_for_ros_type(ros_type),
            (None, Some(name)) => self.find_converter_for_generic_archetype(name),
            (None, None) => Err(ConverterError::UnsupportedConversion {
                name: RerunName::RerunArchetype(ArchetypeName::from("<ANY>")),
                ros_type: None,
            }),
        }
    }

    fn find_converter_both(
        &self,
        ros_type: &ROSTypeName,
        rerun_name: &RerunName,
    ) -> FindConverterResult {
        let rerun_name = fully_qualified_name(rerun_name);
        self.converters
            .get(&(ros_type.clone(), rerun_name.clone()))
            .map(|converter| Ok(converter.clone()))
            .or_else(|| {
                self.generic_converters
                    .get(&rerun_name)
                    .map(|converter| Ok(converter.clone()))
            })
            .unwrap_or(Err(ConverterError::UnsupportedConversion {
                name: rerun_name,
                ros_type: Some(format!("{ros_type}")),
            }))
    }

    fn find_converter_for_generic_archetype(&self, rerun_name: &RerunName) -> FindConverterResult {
        let rerun_name = fully_qualified_name(rerun_name);
        self.generic_converters
            .get(&rerun_name)
            .map(|converter| Ok(converter.clone()))
            .unwrap_or(Err(ConverterError::UnsupportedConversion {
                name: rerun_name,
                ros_type: None,
            }))
    }

    fn find_converter_for_ros_type(&self, ros_type: &ROSTypeName) -> FindConverterResult {
        if let Some(converter) = self.converters_by_ros_type.get(ros_type) {
            Ok(converter.clone())
        } else {
            Err(ConverterError::UnsupportedConversion {
                name: RerunName::RerunArchetype(ArchetypeName::from("<ANY>")),
                ros_type: Some(format!("{ros_type}")),
            })
        }
    }

    pub(crate) fn register<T>(&mut self, converter: &T)
    where
        T: ConverterCfg + Clone + 'static,
    {
        self.register_converter(
            &converter.rerun_name(),
            converter.ros_type(),
            Box::new(converter.clone()) as Box<dyn ConverterCfg>,
        );
    }

    /// Register a conversion from an archetype converter.
    fn register_converter(
        &mut self,
        rerun_name: &RerunName,
        ros_type: Option<&ROSTypeString<'_>>,
        converter: Box<dyn ConverterCfg>,
    ) {
        let parsed_type = ros_type.map(ROSTypeName::try_from).transpose();
        match parsed_type {
            Ok(Some(ros_type)) => {
                debug!("Registered converter for {rerun_name} with ROS type {ros_type}");
                if !self.converters_by_ros_type.contains_key(&ros_type) {
                    self.converters_by_ros_type
                        .insert(ros_type.clone(), converter.clone());
                }
                self.converters
                    .insert((ros_type, rerun_name.clone()), converter);
            }
            Ok(None) => {
                debug!("Registered generic converter for {rerun_name}");
                self.generic_converters
                    .insert(rerun_name.clone(), converter);
            }
            Err(err) => {
                if let Some(ros_type) = ros_type {
                    if self.error_types.contains_key(&ros_type.to_string()) {
                        return;
                    }
                    debug!(
                        "Failed to register converter for {rerun_name}, ROS type {ros_type:?} not found: {err}"
                    );
                    self.error_types.insert(ros_type.to_string(), err);
                }
            }
        };
    }
}

pub(super) type FindConverterResult = Result<Box<dyn ConverterCfg>, ConverterError>;

fn fully_qualified_name(name: &RerunName) -> RerunName {
    match name {
        RerunName::RerunArchetype(name) => {
            if name.starts_with("rerun.archetypes.") {
                RerunName::RerunArchetype(ArchetypeName::from(name.as_str()))
            } else {
                RerunName::RerunArchetype(ArchetypeName::new(
                    format!("rerun.archetypes.{name}").as_str(),
                ))
            }
        }
        _ => name.clone(),
    }
}
