use std::sync::Arc;

use rerun::AsComponents;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// Represents any log data that can be sent between topology components
pub enum LogData {
    Archetype(LogComponents),
    ArchetypeArray(Vec<LogComponents>),
    AnyComponents(LogComponents),
    AnyComponentsArray(Vec<LogComponents>),
}

/// All data for logging a Rerun archetype or custom components
pub struct LogComponents {
    pub entity_path: Arc<str>,
    pub header: Option<Arc<LogHeader>>,
    pub components: Arc<dyn AsComponents + Send + Sync>,
}

/// Header information for log messages
///
/// Maps to the ROS std_msgs/Header definition
/// and used to set the logged timepoint and
/// modify the entity_path for transforms.
#[derive(Default)]
pub struct LogHeader {
    pub frame: Option<String>,
    pub time: rerun::TimePoint,
}

#[derive(Clone)]
pub struct ArchetypeSender {
    pub tx: Vec<UnboundedSender<LogData>>,
}

pub struct ArchetypeReceiver {
    pub rx: UnboundedReceiver<LogData>,
}
