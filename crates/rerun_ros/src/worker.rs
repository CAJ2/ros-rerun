use std::sync::Arc;

use rclrs::DynamicSubscription;

use crate::{
    archetypes::{
        archetype::{ArchetypeConverter, ConverterRegistry},
        ROSTypeName,
    },
    config::TopicSource,
};

pub struct SubscriptionWorker {
    topic: String,
    subscription: DynamicSubscription,
    converter: Arc<Box<dyn ArchetypeConverter>>,
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
        config: &TopicSource,
        registry: &ConverterRegistry,
    ) -> anyhow::Result<Self> {
        let archetype_name = rerun::ArchetypeName::from(config.archetype.as_str());
        // TODO: Handle message type auto-discovery
        let valid_ros_type = config
            .ros_type
            .as_ref()
            .expect("ROS type auto-discovery is not yet implemented");
        let ros_type: ROSTypeName = valid_ros_type.as_str().try_into()?;
        let found_converter = registry.find_converter(&archetype_name, &ros_type)?;
        let converter = Arc::new(found_converter.clone());
        let cb_converter = converter.clone();

        let sub = node.create_dynamic_subscription(
            ros_type.into(),
            config.topic.as_str(),
            move |msg: rclrs::DynamicMessage, _info: rclrs::MessageInfo| {
                let instance = cb_converter.clone();
                tokio::spawn(async move {
                    // TODO: Handle this
                    instance.convert(msg.view()).await;
                });
            },
        )?;

        Ok(Self {
            topic: config.topic.clone(),
            subscription: sub,
            converter,
        })
    }
}
