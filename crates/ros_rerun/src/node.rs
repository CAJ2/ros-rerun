use std::{sync::Arc, time::Duration};

use ahash::{HashMap, HashMapExt as _};
use anyhow::Result;
use log::error;
use parking_lot::Mutex;
use rclrs::{Executor, Node, Promise};
use rerun::external::re_log::error_once;

use crate::{
    archetypes::{archetype::ConverterRegistry, ROSTypeName},
    config::CONFIG,
    topology::{parse_topology_config, TopologyState},
};

/// Encapsulates the ROS2 node
///
/// Handles querying the ROS2 graph for auto-discovery of topics
pub struct NodeGraph {
    node: Node,
    change_notifier: Promise<()>,
    msg_topics: Mutex<HashMap<String, String>>,
    registry: Arc<ConverterRegistry>,
}

impl NodeGraph {
    /// Creates the primary ROS node
    ///
    /// # Errors
    ///
    /// Returns an error if the node creation fails.
    pub fn new(executor: &Executor) -> Result<Self> {
        let node = executor.create_node("ros_rerun_bridge")?;
        let notifier = node.notify_on_graph_change_with_period(Duration::new(1, 0), || true);
        let registry = Arc::new(ConverterRegistry::init());
        let graph = Self {
            node: node.clone(),
            change_notifier: notifier,
            msg_topics: Mutex::new(HashMap::with_capacity(64)),
            registry,
        };

        Ok(graph)
    }

    pub async fn run(mut self) {
        let topology_config = match parse_topology_config(&CONFIG.read()) {
            Ok(config) => config,
            Err(err) => {
                error!("Failed to parse topology config: {err}");
                return;
            }
        };
        let topology = Arc::new(tokio::sync::Mutex::new(TopologyState::default()));
        let node = self.node.clone();
        let registry = self.registry.clone();
        let cloned_topology = topology.clone();
        let topology_handle = tokio::spawn(async move {
            let mut topo = cloned_topology.lock().await;
            if let Err(err) = topo.apply_config(node, &topology_config, &registry).await {
                error!("Failed to apply topology config: {err}");
            }
        });
        let main_loop_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                     _ = &mut self.change_notifier => {
                        if let Err(err) = self.refresh_graph() {
                            error!("Failed to refresh graph: {err}");
                        }
                     }
                }
            }
        });
        if let Err(err) = tokio::join!(main_loop_handle, topology_handle).0 {
            error!("Node graph main loop failed: {err}");
        }
    }

    pub fn get_topic_type(&self, topic: &str) -> Option<ROSTypeName> {
        let msg_topics = self.msg_topics.lock();
        match msg_topics.get(topic) {
            Some(ros_type) => ros_type.as_str().try_into().ok(),
            None => None,
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
                msg_topics.insert(topic.clone(), types.first().expect("No type").clone());
            }
        }
        Ok(())
    }
}
