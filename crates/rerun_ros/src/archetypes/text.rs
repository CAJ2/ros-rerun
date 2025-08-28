use std::vec;

use async_trait::async_trait;
use rclrs::BaseType;
use rerun::{Archetype as _, ArchetypeName};
use serde::{Deserialize, Serialize};

use crate::{
    archetypes::{
        archetype::{ArchetypeConverter, ConverterError, MessageVisitor as _},
        dynamic_message::DynMessageViewCast as _,
        ArchetypeData, ROSTypeName, ROSTypeString,
    },
    config::defs::ConverterSettings,
};

const STD_MSGS_STRING: ROSTypeString<'static> = ROSTypeString("std_msgs", "String");

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct TextDocumentConfig {
    /// The field in the ROS message to extract the text from.
    /// If `None`, it will output all text-like fields.
    field: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct TextDocument {
    ros_types: Vec<ROSTypeName>,
    config: TextDocumentConfig,
}

impl TextDocument {
    pub fn new() -> Self {
        Self {
            ros_types: vec![ROSTypeName::new("std_msgs", "String")],
            config: Default::default(),
        }
    }
}

#[async_trait]
impl ArchetypeConverter for TextDocument {
    fn rerun_name(&self) -> ArchetypeName {
        rerun::TextDocument::name()
    }

    fn ros_types(&self) -> Option<Vec<ROSTypeName>> {
        Some(self.ros_types.clone())
    }

    fn supports_custom(&self) -> bool {
        true
    }

    fn with_config(&mut self, config: ConverterSettings) -> anyhow::Result<(), ConverterError> {
        if let Some(field) = config
            .get("field")
            .and_then(|v| v.as_str().map(str::to_owned))
        {
            self.config.field = Some(field);
        }
        Ok(())
    }

    async fn convert<'a>(
        &self,
        topic: &str,
        ros_type: &ROSTypeName,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> Option<ArchetypeData> {
        match ros_type {
            t if *t == STD_MSGS_STRING => {
                let text = msg.get_string("data")?;
                Some(ArchetypeData::new(
                    topic.to_owned(),
                    Box::new(rerun::TextDocument::new(text)),
                ))
            }
            _ => None,
        }
    }

    async fn convert_custom<'a>(
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
