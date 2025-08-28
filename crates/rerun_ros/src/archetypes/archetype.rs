use ahash::HashMap;
use anyhow::Result;
use async_trait::async_trait;
use rclrs::{BaseType, Value};
use rerun::external::re_types_core::ArchetypeName;

use crate::archetypes::{text::TextDocument, ArchetypeData};

/// Trait for converting ROS messages into Rerun archetypes.
#[async_trait]
pub trait ArchetypeConverter {
    /// Get the name of the Rerun archetype.
    fn rerun_name(&self) -> ArchetypeName;

    /// Process a dynamic message and convert it into a Rerun archetype.
    async fn convert<'a>(
        &self,
        topic: &str,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> Result<ArchetypeData>;
}

pub struct ConverterRepository {
    converters: HashMap<String, Box<dyn ArchetypeConverter>>,
}

pub fn create_archetype_converter(
    name: &str,
    config: toml::Table,
) -> Result<Box<dyn ArchetypeConverter>> {
    let name = fully_qualified_name(name);
    match name.as_str() {
        "rerun.archetypes.TextDocument" => TextDocument::from_toml(config)
            .map(|doc| Ok(Box::new(doc) as Box<dyn ArchetypeConverter>))?,
        _ => Err(anyhow::anyhow!("Unknown archetype: {name}"))?,
    }
}

fn fully_qualified_name(name: &str) -> String {
    if name.starts_with("rerun.archetypes.") {
        name.to_owned()
    } else {
        format!("rerun.archetypes.{name}")
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
