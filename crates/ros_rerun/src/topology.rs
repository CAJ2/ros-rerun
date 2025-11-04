use std::{collections::BTreeMap, fmt::Display};

use ahash::{HashMap, HashMapExt as _, HashSet, HashSetExt as _};
use log::{debug, error};
use stream_cancel::{Trigger, Tripwire};
use thiserror::Error;
use tokio::sync::mpsc::unbounded_channel;

use crate::{
    archetypes::archetype::ConverterRegistry,
    channel::{ArchetypeReceiver, ArchetypeSender, LogData},
    config::{defs::Config, DBConfig, StreamConfig, TopicSource},
    worker::{DBSinkWorker, GRPCSinkWorker, SubscriptionWorker},
};

#[derive(Error, Debug)]
pub enum TopologyConfigError {
    #[error("Duplicate component ID found: {0}")]
    DuplicateID(String),

    #[error("Component {0} cannot define itself as an input")]
    SelfReference(ComponentID),

    #[error("Component {0} failed to initialize")]
    InitializationError(ComponentID),

    #[error("Component {0} failed to initialize the Rerun SDK: {1}")]
    RerunInitializationError(ComponentID, #[source] Box<rerun::RecordingStreamError>),
}

/// Configuration describing the flow of data from ROS topics to Rerun.
///
/// This is derived from a Config struct.
/// To perform runtime modifications to the state, a new `TopologyConfig`
/// will be constructed, compared to the current `TopologyState`, and
/// and changes will be asynchronously applied.
#[derive(Debug)]
pub struct TopologyConfig {
    topic_subscriptions: BTreeMap<ComponentID, TopicSource>,
    grpc_sinks: BTreeMap<ComponentID, String>,
    db_sink: DBConfig,
    edges: BTreeMap<ComponentID, Vec<ComponentID>>,
}

impl TopologyConfig {
    /// Validate the topology configuration.
    ///
    /// # Errors
    ///
    /// May return several different errors in `TopologyConfigError`
    /// if it detects any issues with the topology before attempting to
    /// apply it.
    pub fn validate(&self) -> anyhow::Result<(), TopologyConfigError> {
        self.check_duplicate_ids()?;
        self.check_invalid_edges()?;
        Ok(())
    }

    fn check_duplicate_ids(&self) -> anyhow::Result<(), TopologyConfigError> {
        // Check for duplicate IDs
        let mut seen = HashSet::new();
        self.topic_subscriptions
            .keys()
            .chain(self.grpc_sinks.keys())
            .try_for_each(|k| match k {
                ComponentID::GRPCSink(name) | ComponentID::TopicSubscriber(name) => {
                    if !seen.insert(name) {
                        Err(TopologyConfigError::DuplicateID(name.to_owned()))
                    } else {
                        Ok(())
                    }
                }
                ComponentID::DBSink => Ok(()),
            })?;
        Ok(())
    }

    fn check_invalid_edges(&self) -> anyhow::Result<(), TopologyConfigError> {
        for (sink, sources) in &self.edges {
            if let Some(source) = sources.iter().find(|source| *source == sink) {
                return Err(TopologyConfigError::SelfReference(source.clone()));
            }
        }
        Ok(())
    }
}

/// Parse the topology configuration from the given config.
///
/// # Errors
/// Returns a `TopologyConfigError` if the configuration is invalid.
pub fn parse_topology_config(
    config: &Config,
) -> anyhow::Result<TopologyConfig, TopologyConfigError> {
    let mut topic_subscriptions = BTreeMap::new();
    let mut grpc_sinks = BTreeMap::new();
    let mut edges: BTreeMap<ComponentID, Vec<ComponentID>> = BTreeMap::new();

    for (name, source) in config.topics() {
        let source_id = ComponentID::TopicSubscriber(name.clone());
        topic_subscriptions.insert(source_id.clone(), source.clone());
    }

    // Set up a single default database sink
    let mut db_inputs = Vec::new();
    config.db.inputs.iter().for_each(|input| {
        if topic_subscriptions.contains_key(&ComponentID::TopicSubscriber(input.clone())) {
            db_inputs.push(ComponentID::TopicSubscriber(input.clone()));
        }
    });
    edges.insert(ComponentID::DBSink, db_inputs);

    // Setup gRPC sinks
    for (name, stream) in config.streams() {
        let sink_id = ComponentID::GRPCSink(name.clone());
        grpc_sinks.insert(sink_id.clone(), stream.url.clone());

        // Connect appropriate sources to this sink
        for input in &stream.inputs {
            if topic_subscriptions.contains_key(&ComponentID::TopicSubscriber(input.clone())) {
                edges
                    .entry(sink_id.clone())
                    .or_default()
                    .push(ComponentID::TopicSubscriber(input.clone()));
            } else if grpc_sinks.contains_key(&ComponentID::GRPCSink(input.clone())) {
                return Err(TopologyConfigError::SelfReference(ComponentID::GRPCSink(
                    input.clone(),
                )));
            }
        }
    }

    let topo_cfg = TopologyConfig {
        topic_subscriptions,
        grpc_sinks,
        db_sink: config.db.clone(),
        edges,
    };
    topo_cfg.validate()?;

    Ok(topo_cfg)
}

/// The state of a running topology.
#[derive(Default)]
pub struct TopologyState {
    topic_subscriptions: HashMap<ComponentID, SubscriptionWorker>,
    grpc_sinks: HashMap<ComponentID, GRPCSinkWorker>,
    db_sink: Option<DBSinkWorker>,
    edges: HashMap<ComponentID, InputChannel>,
    shutdown_trigger: Option<Trigger>,
}

impl TopologyState {
    /// Apply a new topology configuration to the current state.
    ///
    /// # Errors
    ///
    /// Returns a `TopologyConfigError` if all or part of the configuration
    /// fails to initialize and start running.
    pub async fn apply_config(
        &mut self,
        node: rclrs::Node,
        config: &TopologyConfig,
        registry: &ConverterRegistry,
    ) -> anyhow::Result<(), TopologyConfigError> {
        let (shutdown_trigger, shutdown) = Tripwire::new();
        self.shutdown_trigger = Some(shutdown_trigger);
        let mut rx_map = HashMap::new();
        // Apply edges
        for (id, channel) in &config.edges {
            let (tx, rx) = unbounded_channel::<LogData>();
            self.edges.insert(
                id.clone(),
                InputChannel {
                    components: channel.clone(),
                    channel: ArchetypeSender { tx: vec![tx] },
                },
            );
            rx_map.insert(id, ArchetypeReceiver { rx });
        }

        // Apply topic subscriptions
        for (id, worker) in &config.topic_subscriptions {
            let connecting_components = self
                .edges
                .iter()
                .filter_map(|(edge_id, input)| {
                    if input.components.contains(id) {
                        Some(edge_id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let input_channel = connecting_components
                .iter()
                .map(|comp_id| {
                    self.edges
                        .get(comp_id)
                        .map(|input| input.channel.clone())
                        .expect("No channel for component")
                })
                .collect::<Vec<_>>();
            // Create a new SubscriptionWorker
            let subscription_worker = SubscriptionWorker::new(
                &node,
                worker,
                registry,
                ArchetypeSender {
                    tx: input_channel
                        .iter()
                        .map(|ch| ch.tx.first().expect("No tx channel").clone())
                        .collect::<Vec<_>>(),
                },
            )
            .map_err(|_err| TopologyConfigError::InitializationError(id.clone()))?;
            self.topic_subscriptions
                .insert(id.clone(), subscription_worker);
        }

        // Apply GRPC sinks
        for (id, url) in &config.grpc_sinks {
            let rx_channel = rx_map.remove(id).expect("No channel for component");
            // Create a new GRPCSinkWorker
            let grpc_sink_worker = GRPCSinkWorker::new(&StreamConfig {
                url: url.clone(),
                inputs: vec![],
            })
            .map_err(|_err| TopologyConfigError::InitializationError(id.clone()))?;
            grpc_sink_worker.run(rx_channel, shutdown.clone());
            self.grpc_sinks.insert(id.clone(), grpc_sink_worker);
        }

        // Apply DB sink
        let rx_channel = rx_map
            .remove(&ComponentID::DBSink)
            .expect("No channel for component");
        let db_sink_worker = DBSinkWorker::new(&config.db_sink)
            .map_err(|_err| TopologyConfigError::InitializationError(ComponentID::DBSink))?;
        db_sink_worker.run(rx_channel, shutdown.clone());
        self.db_sink = Some(db_sink_worker);

        debug!("Applied topology config {config:?}");
        Ok(())
    }
}

struct InputChannel {
    components: Vec<ComponentID>,
    channel: ArchetypeSender,
}

/// Unique identifier for a component in the system.
///
/// The topology uses these identifiers to route archetypes
/// from inputs to sinks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ComponentID {
    TopicSubscriber(String),
    GRPCSink(String),
    DBSink,
}

impl Display for ComponentID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopicSubscriber(name) => write!(f, "Message subscriber '{name}'"),
            Self::GRPCSink(name) => write!(f, "Rerun SDK stream '{name}'"),
            Self::DBSink => write!(f, "Database"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config;
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn default_topology() {
        let cfg = config::Config::default();
        let topology = parse_topology_config(&cfg);
        assert!(topology.is_ok());
    }

    #[test]
    fn valid_topology() {
        let cfg = config::Config {
            topics: HashMap::from([(
                "comp1".into(),
                config::TopicSource {
                    topic: "example_topic".into(),
                    ros_type: Some("std_msgs/String".into()),
                    archetype: "TextLog".into(),
                    ..Default::default()
                },
            )]),
            streams: HashMap::from([(
                "stream1".into(),
                config::StreamConfig {
                    url: "http://localhost:8080".parse().expect("Invalid address"),
                    inputs: vec![],
                },
            )]),
            ..Default::default()
        };
        let topology = parse_topology_config(&cfg);
        assert!(topology.is_ok());
    }

    #[test]
    fn invalid_duplicates() {
        let cfg = config::Config {
            topics: HashMap::from([(
                "comp1".into(),
                config::TopicSource {
                    topic: "example_topic".into(),
                    ros_type: Some("std_msgs/String".into()),
                    archetype: "TextLog".into(),
                    ..Default::default()
                },
            )]),
            streams: HashMap::from([(
                "comp1".into(),
                config::StreamConfig {
                    url: "http://localhost:8080".parse().expect("Invalid address"),
                    inputs: vec![],
                },
            )]),
            ..Default::default()
        };
        let topology = parse_topology_config(&cfg);
        assert!(topology.is_err());
    }

    #[test]
    fn invalid_self_referencing() {
        let cfg = config::Config {
            topics: HashMap::from([(
                "comp1".into(),
                config::TopicSource {
                    topic: "example_topic".into(),
                    ros_type: Some("std_msgs/String".into()),
                    archetype: "TextLog".into(),
                    ..Default::default()
                },
            )]),
            streams: HashMap::from([
                (
                    "stream1".into(),
                    config::StreamConfig {
                        url: "http://localhost:8080".parse().expect("Invalid address"),
                        inputs: vec!["stream1".into(), "comp1".into()],
                    },
                ),
                (
                    "stream2".into(),
                    config::StreamConfig {
                        url: "http://localhost:8080".parse().expect("Invalid address"),
                        inputs: vec!["stream1".into(), "comp1".into()],
                    },
                ),
            ]),
            ..Default::default()
        };
        let topology = parse_topology_config(&cfg);
        assert!(topology.is_err());
    }
}
