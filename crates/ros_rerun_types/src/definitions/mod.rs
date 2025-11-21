//! ROS2 message definitions.
//!
//! This module provides `serde`-compatible Rust types for a subset of
//! ROS2 message definitions, allowing for deserialization of MCAP files containing
//! ROS2 data into idiomatic Rust structs.
//!
//! Based on the definitions from Rerun: https://github.com/rerun-io/rerun/tree/main/crates/utils/re_mcap/src/parsers/ros2msg/definitions

pub mod builtin_interfaces;
pub mod geometry_msgs;
pub mod rcl_interfaces;
pub mod sensor_msgs;
pub mod std_msgs;
