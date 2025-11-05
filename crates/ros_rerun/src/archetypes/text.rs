use std::sync::Arc;

use async_trait::async_trait;
use rclrs::BaseType;
use rerun::{Archetype as _, ArchetypeName};
use serde::{Deserialize, Serialize};

use crate::{
    archetypes::{
        archetype::{ArchetypeConverter, ConverterConfigurable, ConverterError},
        dynamic_message::MessageVisitor as _,
        ROSTypeName, ROSTypeString,
    },
    channel::{LogComponents, LogData},
    config::defs::ConverterSettings,
};

const STD_MSGS_STRING: ROSTypeString<'_> = ROSTypeString("std_msgs", "String");
const TEXT_DOCUMENT_TYPES: &[ROSTypeString<'_>] = &[STD_MSGS_STRING];

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct TextDocumentConfig {
    /// The field in the ROS message to extract the text from.
    /// If `None`, it will output all text-like fields.
    field: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct TextDocument {
    topic: Arc<str>,
    ros_type: Option<ROSTypeName>,
    config: TextDocumentConfig,
}

impl ConverterConfigurable for TextDocument {
    fn set_topic(&mut self, topic: &str) {
        self.topic = Arc::from(topic);
    }

    fn set_ros_type(&mut self, ros_type: Option<ROSTypeName>) {
        self.ros_type = ros_type;
    }

    fn set_config(&mut self, config: ConverterSettings) -> anyhow::Result<(), ConverterError> {
        if let Some(field) = config
            .get("field")
            .and_then(|v| v.as_str().map(str::to_owned))
        {
            self.config.field = Some(field);
        }
        Ok(())
    }
}

#[async_trait]
impl ArchetypeConverter for TextDocument {
    fn rerun_name(&self) -> ArchetypeName {
        rerun::TextDocument::name()
    }

    fn ros_types(&self) -> Option<Vec<ROSTypeString<'static>>> {
        Some(TEXT_DOCUMENT_TYPES.to_vec())
    }

    fn supports_custom(&self) -> bool {
        true
    }

    async fn convert<'a>(
        &self,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> anyhow::Result<LogData, ConverterError> {
        match &self.ros_type {
            Some(t) if *t == STD_MSGS_STRING => {
                if let Some(text) = msg.get_string("data") {
                    Ok(LogData::Archetype(LogComponents {
                        entity_path: self.topic.clone(),
                        header: None,
                        components: Arc::new(rerun::TextDocument::new(text)),
                    }))
                } else {
                    Err(ConverterError::ConversionError(
                        self.rerun_name(),
                        t.to_string(),
                        anyhow::anyhow!("Missing 'data' field"),
                    ))
                }
            }
            None => {
                let text = msg
                    .iter_by_type(BaseType::String)
                    .map(|value| match value {
                        rclrs::Value::Simple(rclrs::SimpleValue::String(value)) => {
                            value.to_string()
                        }
                        _ => rosidl_runtime_rs::String::default().to_string(),
                    })
                    .reduce(|mut acc, item| {
                        acc.push_str(&item);
                        acc
                    })
                    .unwrap_or_default();
                Ok(LogData::Archetype(LogComponents {
                    entity_path: self.topic.clone(),
                    header: None,
                    components: Arc::new(rerun::TextDocument::new(text)),
                }))
            }
            _ => Err(ConverterError::UnsupportedConversion {
                name: self.rerun_name(),
                ros_type: self.ros_type.as_ref().map(|t| t.to_string()),
            }),
        }
    }
}
