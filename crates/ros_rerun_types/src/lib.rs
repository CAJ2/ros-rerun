//! This crate provides converters between ROS message types and Rerun archetypes/components.
//! It is a supporting crate for the `ros_rerun` crate.

use rerun::ArchetypeName;
use std::fmt::Display;

pub mod converters;

pub mod converter;
pub mod dynamic_message;
pub mod register;

/// Represents a runtime-checked ROS message type.
///
/// This ensures that the ROS type definition is available.
/// It is a wrapper around the `rclrs::MessageTypeName` type,
/// but with some other goodies like Hash and conversion/comparison with
/// the unchecked `ROSTypeString` type.
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

impl TryFrom<&ROSTypeString<'_>> for ROSTypeName {
    type Error = rclrs::dynamic_message::DynamicMessageError;

    fn try_from(value: &ROSTypeString<'_>) -> Result<Self, Self::Error> {
        rclrs::dynamic_message::MessageTypeName::try_from(
            format!("{}/msg/{}", value.0, value.1).as_str(),
        )
        .map(ROSTypeName)
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
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
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

impl Display for ROSTypeString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() && self.1.is_empty() {
            write!(f, "<ANY>")
        } else {
            write!(f, "{}/msg/{}", self.0, self.1)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RerunName {
    RerunArchetype(ArchetypeName),
    ROSArchetype(ArchetypeName),
    Components,
}

impl Display for RerunName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RerunArchetype(name) => write!(f, "{name} (Rerun archetype)"),
            Self::ROSArchetype(name) => write!(f, "{name} (ROS archetype)"),
            Self::Components => write!(f, "<Rerun components>"),
        }
    }
}
