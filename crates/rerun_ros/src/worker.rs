use std::sync::Arc;

use rclrs::DynamicSubscription;

use crate::archetypes::archetype::ArchetypeConverter;

pub struct SubscriptionWorker {
    topic: String,
    subscription: DynamicSubscription,
    converter: Arc<Box<dyn ArchetypeConverter + Send + Sync + 'static>>,
}

impl SubscriptionWorker {
    pub fn new(
        node: &rclrs::Node,
        topic: &str,
        ros_type: rclrs::MessageTypeName,
        converter: Box<dyn ArchetypeConverter + Send + Sync + 'static>,
    ) -> anyhow::Result<Self> {
        let converter = Arc::new(converter);

        let instance = Arc::clone(&converter);
        let sub_topic = topic.to_owned();
        let sub = node.create_dynamic_subscription(
            ros_type,
            &topic,
            move |msg: rclrs::DynamicMessage, _info: rclrs::MessageInfo| {
                instance.convert(&sub_topic, msg.view());
            },
        )?;

        Ok(Self {
            topic: topic.to_owned(),
            subscription: sub,
            converter,
        })
    }
}
