use std::time::Duration;

use ahash::{HashMap, HashMapExt as _};
use anyhow::Result;
use log::error;
use parking_lot::Mutex;
use rclrs::{Executor, Node, Promise, RclrsError};
use rerun::external::re_log::error_once;

use crate::archetypes::archetype::ConverterRegistry;

/// Encapsulates the ROS2 node
///
/// Handles querying the ROS2 graph for auto-discovery of topics
pub struct NodeGraph {
    node: Node,
    change_notifier: Promise<()>,
    msg_topics: Mutex<HashMap<String, String>>,
}

impl NodeGraph {
    /// Creates the primary ROS node
    ///
    /// # Errors
    /// Returns an error if the node creation fails.
    pub fn new(executor: &Executor) -> Result<Self, RclrsError> {
        let node = executor.create_node("rerun_ros_bridge")?;
        let notifier = node.notify_on_graph_change_with_period(Duration::new(1, 0), || true);
        let _registry = ConverterRegistry::init();
        let graph = Self {
            node,
            change_notifier: notifier,
            msg_topics: Mutex::new(HashMap::with_capacity(64)),
        };

        Ok(graph)
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                 _ = &mut self.change_notifier => {
                    if let Err(err) = self.refresh_graph() {
                        error!("Failed to refresh graph: {err}");
                    }
                 }
            }
        }
    }

    fn refresh_graph(&self) -> Result<()> {
        let topics_and_types = self.node.get_topic_names_and_types()?;
        let topics_and_types: Vec<_> = topics_and_types.into_iter().collect();
        let mut msg_topics = self.msg_topics.lock();
        msg_topics.clear();
        for (topic, types) in &topics_and_types {
            if types.len() > 1 {
                error_once!("Topic {topic} has multiple types, ignoring: {types:?}");
            } else {
                msg_topics.insert(topic.to_string(), types[0].clone());
            }
        }
        Ok(())
    }
}
