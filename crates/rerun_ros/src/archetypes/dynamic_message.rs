use rclrs::{BaseType, DynamicMessageView, Value};

/// Provides methods for easier access to fields in a dynamic message.
pub trait MessageVisitor {
    fn iter_by_type(&self, value_type: BaseType) -> impl Iterator<Item = Value<'_>>;

    fn get_string(&self, field_name: &str) -> Option<String>;
}

impl MessageVisitor for DynamicMessageView<'_> {
    fn iter_by_type(&self, value_type: BaseType) -> impl Iterator<Item = Value<'_>> {
        self.fields.iter().filter_map(move |field| {
            if field.base_type != value_type {
                return None;
            }
            let field_value = self.get(&field.name)?;
            Some(field_value)
        })
    }

    fn get_string(&self, field_name: &str) -> Option<String> {
        match self.get(field_name) {
            Some(rclrs::Value::Simple(rclrs::SimpleValue::String(s))) => Some(s.to_string()),
            _ => None,
        }
    }
}
