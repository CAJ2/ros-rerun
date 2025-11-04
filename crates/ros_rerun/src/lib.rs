//! ROS <-> Rerun integration
//! This crate provides functionality to connect ROS2 to Rerun,
//! primarily for visualization of robot data using the Rerun viewer.
//!
//! The primary use case for the `ros_rerun` bridge is to provide a binary
//! that can be installed on a ROS2 system to stream data to Rerun viewer(s),
//! often over a network, or to record data locally for later playback.

pub mod archetypes;
pub mod ros_introspection;

pub mod channel;
pub mod cli;
pub mod config;
pub mod node;
pub mod topology;
pub mod worker;
