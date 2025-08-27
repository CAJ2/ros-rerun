use std::sync::Arc;

use rclrs::DynamicSubscription;

use crate::archetypes::archetype::ArchetypeTransformer;

pub struct SubscriptionWorker {
    topic: String,
    subscription: DynamicSubscription,
    transformer: Arc<Box<dyn ArchetypeTransformer + Send + Sync + 'static>>,
}

impl SubscriptionWorker {
    pub fn new(
        node: &rclrs::Node,
        topic: &str,
        ros_type: rclrs::MessageTypeName,
        transformer: Box<dyn ArchetypeTransformer + Send + Sync + 'static>,
    ) -> anyhow::Result<Self> {
        let transformer = Arc::new(transformer);

        let instance = Arc::clone(&transformer);
        let sub_topic = topic.to_owned();
        let sub = node.create_dynamic_subscription(
            ros_type,
            &topic,
            move |msg: rclrs::DynamicMessage, _info: rclrs::MessageInfo| {
                instance.transform(&sub_topic, msg.view());
            },
        )?;

        Ok(Self {
            topic: topic.to_owned(),
            subscription: sub,
            transformer,
        })
    }
}
