use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::archetypes::ArchetypeData;

pub struct ArchetypeSender {
    sender: UnboundedSender<ArchetypeData>,
}

pub struct ArchetypeReceiver {
    receiver: UnboundedReceiver<ArchetypeData>,
}
