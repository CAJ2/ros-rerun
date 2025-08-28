use ahash::{HashMap, HashMapExt as _};
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use rclrs::{BaseType, DynamicMessageError, Value};
use rerun::external::re_types_core::ArchetypeName;
use thiserror::Error;

use crate::{
    archetypes::{text::TextDocument, ArchetypeData, ROSTypeName},
    config::defs::ConverterSettings,
};

#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("No converter found for archetype {0} and ROS type {1}")]
    NoConverter(ArchetypeName, String),

    #[error("Unable to parse conversion config for archetype {0} and ROS type {1}")]
    ConfigParseError(ArchetypeName, String),

    #[error("Conversion error for archetype {0} and ROS type {1}: {2}")]
    ConversionError(ArchetypeName, String, anyhow::Error),
}

/// Trait for converting ROS messages into Rerun archetypes.
#[async_trait]
pub trait ArchetypeConverter {
    /// Get the name of the Rerun archetype.
    fn rerun_name(&self) -> ArchetypeName;

    /// Get the ROS message types that this converter can process.
    fn ros_types(&self) -> Option<Vec<ROSTypeName>> {
        None
    }

    fn supports_custom(&self) -> bool {
        false
    }

    fn with_config(&mut self, _config: ConverterSettings) -> anyhow::Result<(), ConverterError> {
        Ok(())
    }

    /// Convert a specific ROS message type to a Rerun archetype.
    ///
    /// Conversions to specific ROS message types are intended
    /// to never fail, but it is possible for them to return `None`
    /// if the message does not match the expected format.
    /// In this case they will be dropped. This may change in the future.
    async fn convert<'a>(
        &self,
        topic: &str,
        ros_type: &ROSTypeName,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> Option<ArchetypeData>;

    /// Convert a custom message type to a Rerun archetype.
    ///
    /// Conversions to custom message types can easily fail,
    /// and errors can be tracked or logged.
    async fn convert_custom<'a>(
        &self,
        topic: &str,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> Result<ArchetypeData>;
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
    // If the convert supports a general conversion, it will be registered with (ArchetypeName, None)
    converters: HashMap<(ArchetypeName, Option<ROSTypeName>), Box<dyn ArchetypeConverter>>,
    error_types: HashMap<(ArchetypeName, String), Vec<DynamicMessageError>>,
}

impl ConverterRegistry {
    pub fn init() -> Self {
        let mut registry = Self {
            converters: HashMap::new(),
            error_types: HashMap::new(),
        };
        registry.register(&TextDocument::new());
        registry
    }

    pub fn register<T>(&mut self, converter: &T)
    where
        T: ArchetypeConverter + Clone + 'static,
    {
        let archetype_name = converter.rerun_name();
        if converter.supports_custom() {
            self.register_converter(
                archetype_name,
                None,
                Box::new(converter.clone()) as Box<dyn ArchetypeConverter>,
            );
        }
        let ros_types = converter.ros_types();
        if let Some(ros_types) = &ros_types {
            for ros_type in ros_types {
                self.register_converter(
                    archetype_name,
                    Some(&ros_type.to_string()),
                    Box::new(converter.clone()) as Box<dyn ArchetypeConverter>,
                );
            }
        }
    }

    // Register a conversion from an archetype converter.
    fn register_converter(
        &mut self,
        archetype_name: ArchetypeName,
        ros_type: Option<&str>,
        converter: Box<dyn ArchetypeConverter>,
    ) {
        let mut error_types: HashMap<(ArchetypeName, &str), Vec<DynamicMessageError>> =
            HashMap::new();
        let parsed_type = ros_type.map(rclrs::MessageTypeName::try_from).transpose();
        match parsed_type {
            Ok(Some(ros_type)) => {
                debug!("Registered converter for {archetype_name} with ROS type {ros_type}");
                self.converters
                    .insert((archetype_name, Some(ROSTypeName(ros_type))), converter);
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
                    error_types.insert((archetype_name, ros_type), vec![err]);
                }
            }
        };
    }
}

pub trait MessageVisitor {
    fn iter_by_type(&self, value_type: BaseType) -> impl Iterator<Item = Value<'_>>;
}

impl MessageVisitor for rclrs::DynamicMessageView<'_> {
    fn iter_by_type(&self, value_type: BaseType) -> impl Iterator<Item = Value<'_>> {
        self.fields.iter().filter_map(move |field| {
            if field.base_type != value_type {
                return None;
            }
            let field_value = self.get(&field.name)?;
            Some(field_value)
        })
    }
}
