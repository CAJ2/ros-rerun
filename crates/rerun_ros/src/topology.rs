use std::{fmt::Display, path::PathBuf};

use ahash::{HashMap, HashMapExt as _, HashSet, HashSetExt as _};
use thiserror::Error;

use crate::{
    channel::ArchetypeSender,
    config::{defs::Config, TopicSource},
    worker::SubscriptionWorker,
};

pub(crate) enum TaskOutput {
    ArchetypeLog,
}

type TaskResult = Result<TaskOutput, anyhow::Error>;

type ComponentHandle = tokio::task::JoinHandle<TaskResult>;

#[derive(Debug, Default)]
struct DBSinkConfig {
    path: PathBuf,
}

#[derive(Error, Debug)]
pub enum TopologyConfigError {
    #[error("Duplicate component ID found: {0}")]
    DuplicateID(String),

    #[error("Component {0} cannot define itself as an input")]
    SelfReference(ComponentID),
}

/// Configuration describing the flow of data from ROS topics to Rerun.
///
/// This is derived from a Config struct.
/// To perform runtime modifications to the state, a new `TopologyConfig`
/// will be constructed, compared to the current `TopologyState`, and
/// and changes will be asynchronously applied.
#[derive(Debug)]
pub struct TopologyConfig {
    topic_subscriptions: HashMap<ComponentID, TopicSource>,
    grpc_sinks: HashMap<ComponentID, String>,
    db_sink: DBSinkConfig,
    edges: HashMap<ComponentID, Vec<ComponentID>>,
}

impl TopologyConfig {
    /// Validate the topology configuration.
    pub fn validate(&self) -> anyhow::Result<(), TopologyConfigError> {
        self.check_duplicate_ids()?;
        self.check_invalid_edges()?;
        Ok(())
    }

    fn check_duplicate_ids(&self) -> anyhow::Result<(), TopologyConfigError> {
        // Check for duplicate IDs
        let mut seen = HashSet::new();
        for id in self
            .topic_subscriptions
            .keys()
            .chain(self.grpc_sinks.keys())
            .map(|k| match k {
                ComponentID::GRPCSink(name) | ComponentID::TopicSubscriber(name) => name,
                _ => "",
            })
        {
            if !seen.insert(id) {
                return Err(TopologyConfigError::DuplicateID(id.to_owned()));
            }
        }
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
    let mut topic_subscriptions = HashMap::new();
    let mut grpc_sinks = HashMap::new();
    let mut edges: HashMap<ComponentID, Vec<ComponentID>> = HashMap::new();

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

    // Set up gRPC sinks
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
        db_sink: DBSinkConfig {
            path: config.db.data_dir.clone(),
        },
        edges,
    };
    topo_cfg.validate()?;

    Ok(topo_cfg)
}

/// The state of a running topology.
pub struct TopologyState {
    topic_subscriptions: HashMap<ComponentID, SubscriptionWorker>,
    // grpc_sinks: HashMap<ComponentID, GRPCSinkWorker>,
    // db_sink: DBSinkWorker,
    edges: HashMap<ComponentID, InputChannel>,
}

struct InputChannel {
    components: Vec<ComponentID>,
    channel: ArchetypeSender,
}

/// Unique identifier for a component in the system.
///
/// The topology uses these identifiers to route archetypes
/// from inputs to sinks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
                    url: "http://localhost:8080".into(),
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
                        url: "http://localhost:8080".into(),
                        inputs: vec!["stream1".into(), "comp1".into()],
                    },
                ),
                (
                    "stream2".into(),
                    config::StreamConfig {
                        url: "http://localhost:8080".into(),
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
