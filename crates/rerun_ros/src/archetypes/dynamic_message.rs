use rclrs::DynamicMessageView;

pub trait DynMessageViewCast {
    fn get_string(&self, field_name: &str) -> Option<String>;
}

impl DynMessageViewCast for DynamicMessageView<'_> {
    fn get_string(&self, field_name: &str) -> Option<String> {
        match self.get(field_name) {
            Some(rclrs::Value::Simple(rclrs::SimpleValue::String(s))) => Some(s.to_string()),
            _ => None,
        }
    }
}
