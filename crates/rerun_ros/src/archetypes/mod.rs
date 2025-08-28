use std::fmt::Display;

use rerun::AsComponents;

pub mod archetype;
pub mod dynamic_message;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ROSTypeName(rclrs::MessageTypeName);

impl ROSTypeName {
    pub fn new(package_name: &'static str, type_name: &'static str) -> Self {
        Self(rclrs::MessageTypeName {
            package_name: package_name.to_owned(),
            type_name: type_name.to_owned(),
        })
    }
}

impl TryFrom<&str> for ROSTypeName {
    type Error = rclrs::dynamic_message::DynamicMessageError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        rclrs::dynamic_message::MessageTypeName::try_from(value).map(ROSTypeName)
    }
}

impl From<ROSTypeName> for rclrs::MessageTypeName {
    fn from(value: ROSTypeName) -> Self {
        value.0
    }
}

impl std::hash::Hash for ROSTypeName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.package_name.hash(state);
        self.0.type_name.hash(state);
    }
}

impl Display for ROSTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Represents an unchecked ROS message type.
///
/// This is meant for constant references to ROS message types.
/// We do not know until runtime whether a type definition
/// is actually available, so use `ROSTypeName` for validation.
pub struct ROSTypeString<'a>(&'a str, &'a str);

impl PartialEq<ROSTypeString<'_>> for ROSTypeName {
    fn eq(&self, other: &ROSTypeString<'_>) -> bool {
        self.0.package_name == other.0 && self.0.type_name == other.1
    }
}

impl PartialEq<ROSTypeName> for ROSTypeString<'_> {
    fn eq(&self, other: &ROSTypeName) -> bool {
        self.0 == other.0.package_name && self.1 == other.0.type_name
    }
}
