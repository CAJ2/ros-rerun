use std::sync::Arc;

use rclrs::DynamicSubscription;

use crate::archetypes::{archetype::ArchetypeConverter, ROSTypeName};

pub struct SubscriptionWorker {
    topic: String,
    subscription: DynamicSubscription,
    transformer: Arc<Box<dyn ArchetypeConverter + Send + Sync + 'static>>,
}

impl SubscriptionWorker {
    /// Create a new subscription worker.
    ///
    /// This will create a new subscription to the specified ROS topic and
    /// set up the necessary message transformation.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription cannot be created.
    pub fn new(
        node: &rclrs::Node,
        topic: &str,
        ros_type: ROSTypeName,
        transformer: Box<dyn ArchetypeConverter + Send + Sync + 'static>,
    ) -> anyhow::Result<Self> {
        let transformer = Arc::new(transformer);

        let instance = Arc::clone(&transformer);
        let sub_topic = topic.to_owned();
        let sub = node.create_dynamic_subscription(
            ros_type.clone().into(),
            topic,
            move |msg: rclrs::DynamicMessage, _info: rclrs::MessageInfo| {
                let instance = Arc::clone(&instance);
                let sub_topic = sub_topic.clone();
                let ros_type = ros_type.clone();
                tokio::spawn(async move {
                    instance.convert(&sub_topic, &ros_type, msg.view()).await;
                });
            },
        )?;

        Ok(Self {
            topic: topic.to_owned(),
            subscription: sub,
            transformer,
        })
    }
}
