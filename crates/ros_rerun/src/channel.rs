use std::sync::Arc;

use rerun::AsComponents;
use ros_rerun_types::converter::Header;
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
    pub entity_path: Arc<String>,
    pub header: Option<Arc<Header>>,
    pub components: Arc<dyn AsComponents + Send + Sync>,
}

#[derive(Clone)]
pub struct ArchetypeSender {
    pub tx: Vec<UnboundedSender<LogData>>,
}

pub struct ArchetypeReceiver {
    pub rx: UnboundedReceiver<LogData>,
}
