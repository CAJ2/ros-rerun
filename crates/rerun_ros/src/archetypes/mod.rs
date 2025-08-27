use rerun::AsComponents;

pub mod archetype;
pub mod text;

pub struct ArchetypeData {
    entity_path: String,
    archetype: Box<dyn AsComponents>,
}

impl ArchetypeData {
    pub fn new(entity_path: String, archetype: Box<dyn AsComponents>) -> Self {
        Self {
            entity_path,
            archetype,
        }
    }
}
