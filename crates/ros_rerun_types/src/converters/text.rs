use std::sync::Arc;

use async_trait::async_trait;
use rclrs::BaseType;
use rerun::Archetype as _;
use serde::{Deserialize, Serialize};

use crate::{
    converter::{Converter, ConverterCfg, ConverterData, ConverterError, ConverterSettings},
    dynamic_message::MessageVisitor as _,
    ROSTypeString, RerunName,
};

const STD_MSGS_STRING: ROSTypeString<'_> = ROSTypeString("std_msgs", "String");

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct TextDocumentConfig {
    /// The field in the ROS message to extract the text from.
    /// If `None`, it will output all text-like fields.
    field: Option<String>,
}

impl TextDocumentConfig {
    fn parse(
        &mut self,
        config: &ConverterSettings,
        rerun_name: RerunName,
        ros_type: &ROSTypeString<'_>,
    ) -> anyhow::Result<(), ConverterError> {
        let field = config.0.get("field");
        if let Some(field) = field {
            let field_str = field.as_str().ok_or(ConverterError::InvalidConfig(
                rerun_name,
                ros_type.to_string(),
                anyhow::anyhow!("'field' must be a string"),
            ))?;
            self.field = Some(field_str.to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct StdStringToTextDocument {}

impl ConverterCfg for StdStringToTextDocument {
    fn set_config(&mut self, config: ConverterSettings) -> anyhow::Result<(), ConverterError> {
        if !config.0.is_empty() {
            Err(ConverterError::InvalidConfig(
                self.rerun_name(),
                STD_MSGS_STRING.to_string(),
                anyhow::anyhow!("StdStringToTextDocument does not accept any configuration"),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Converter for StdStringToTextDocument {
    fn rerun_name(&self) -> RerunName {
        RerunName::RerunArchetype(rerun::TextDocument::name())
    }

    fn ros_type(&self) -> Option<&ROSTypeString<'static>> {
        Some(&STD_MSGS_STRING)
    }

    async fn convert_view<'a>(
        &self,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> anyhow::Result<ConverterData, ConverterError> {
        if let Some(text) = msg.get_string("data") {
            Ok(ConverterData {
                header: None,
                components: Arc::new(rerun::TextDocument::new(text)),
            })
        } else {
            Err(ConverterError::Conversion(
                self.rerun_name(),
                STD_MSGS_STRING.to_string(),
                anyhow::anyhow!("Missing 'data' field"),
            ))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AnyToTextDocument {
    config: TextDocumentConfig,
}

impl ConverterCfg for AnyToTextDocument {
    fn set_config(&mut self, config: ConverterSettings) -> anyhow::Result<(), ConverterError> {
        self.config = TextDocumentConfig::default();
        self.config
            .parse(&config, self.rerun_name(), &ROSTypeString::default())
    }
}

#[async_trait]
impl Converter for AnyToTextDocument {
    fn rerun_name(&self) -> RerunName {
        RerunName::RerunArchetype(rerun::TextDocument::name())
    }

    fn ros_type(&self) -> Option<&ROSTypeString<'static>> {
        None
    }

    async fn convert_view<'a>(
        &self,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> anyhow::Result<ConverterData, ConverterError> {
        let text = msg
            .iter_by_type(BaseType::String)
            .map(|value| match value {
                rclrs::Value::Simple(rclrs::SimpleValue::String(value)) => value.to_string(),
                _ => String::default(),
            })
            .reduce(|mut acc, item| {
                acc.push_str(&item);
                acc
            })
            .unwrap_or_default();
        Ok(ConverterData {
            header: None,
            components: Arc::new(rerun::TextDocument::new(text)),
        })
    }
}
