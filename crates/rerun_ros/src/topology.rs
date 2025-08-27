use ahash::HashMap;
use rclrs::WorkerSubscription;

use crate::{channel::ArchetypeSender, worker::SubscriptionWorker};

pub(crate) enum TaskOutput {
    ArchetypeLog,
}

type TaskResult = Result<TaskOutput, anyhow::Error>;

type ComponentHandle = tokio::task::JoinHandle<TaskResult>;

pub enum TopicSubscription {
    TopicAndType(String, String),
    Topic(String),
}

/// Configuration describing the flow of data from ROS topics to Rerun.
///
/// This is derived from a Config struct.
/// To perform runtime modifications to the state, a new `TopologyConfig`
/// will be constructed, compared to the current `TopologyState`, and
/// and changes will be asynchronously applied.
pub struct TopologyConfig {
    topic_subscriptions: HashMap<ComponentID, TopicSubscription>,
    file_sinks: HashMap<ComponentID, String>,
    grpc_sinks: HashMap<ComponentID, String>,
    edges: HashMap<ComponentID, Vec<ComponentID>>,
}

pub struct TopologyState {
    topic_subscriptions: HashMap<ComponentID, SubscriptionWorker>,
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
pub enum ComponentID {
    TopicSubscriber(String),
    Sink(String),
}
