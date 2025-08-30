use stream_cancel::Tripwire;
use tokio::sync::mpsc::{error::SendError, UnboundedReceiver, UnboundedSender};

use crate::archetypes::ArchetypeData;

#[derive(Clone)]
pub struct ArchetypeSender {
    pub tx: Vec<UnboundedSender<ArchetypeData>>,
}

pub struct ArchetypeReceiver {
    pub rx: UnboundedReceiver<ArchetypeData>,
}
