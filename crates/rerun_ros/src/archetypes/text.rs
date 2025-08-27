use async_trait::async_trait;
use rclrs::BaseType;
use rerun::{Archetype, ArchetypeName};
use serde::{Deserialize, Serialize};

use crate::archetypes::{
    archetype::{ArchetypeTransformer, MessageVisitor},
    ArchetypeData,
};

#[derive(Deserialize, Serialize, Default)]
pub struct TextDocumentConfig {
    /// The field in the ROS message to extract the text from.
    /// If `None`, it will output all text-like fields.
    field: Option<String>,
}

pub struct TextDocument {
    config: TextDocumentConfig,
}

impl TextDocument {
    pub fn from_toml(config: toml::Table) -> anyhow::Result<Self> {
        let mut arch = Self {
            config: Default::default(),
        };
        arch.config = config.try_into()?;
        Ok(arch)
    }
}

#[async_trait]
impl ArchetypeTransformer for TextDocument {
    fn rerun_name(&self) -> ArchetypeName {
        rerun::TextDocument::name()
    }

    async fn transform<'a>(
        &self,
        topic: &str,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> anyhow::Result<ArchetypeData> {
        let text = msg
            .iter_by_type(BaseType::String)
            .map(|value| match value {
                rclrs::Value::Simple(rclrs::SimpleValue::String(value)) => value.to_string(),
                _ => rosidl_runtime_rs::String::default().to_string(),
            })
            .reduce(|mut acc, item| {
                acc.push_str(&item);
                acc
            })
            .unwrap_or_default();
        Ok(ArchetypeData::new(
            topic.to_owned(),
            Box::new(rerun::TextDocument::new(text)),
        ))
    }
}
