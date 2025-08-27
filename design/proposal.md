# Project Proposal

## Motivation
There are many current and potential users of Rerun who use ROS2. Right now, the only way to get data from a ROS2 system is to write a custom script in a Rerun-supported language that logs ROS2 data using the Rerun logging API.

A primary usecase for Rerun is live monitoring of a robot or some other system using ROS2. Other tools such as Foxglove provide both a viewer and an agent that can communicate over a network and provide out-of-the-box support for common ROS2 messages, along with easy viewer configuration options. If Rerun could support a ROS2 agent with runtime-configurable live monitoring of a ROS2 system, this can significantly increase adoption of Rerun.

## Goals

- Single binary written in Rust that can be deployed to any system running ROS2
- Connect to any number of Rerun viewers over GRPC
- Auto-discovery of ROS2 message topics
- Exposes a GRPC server allowing for live configuration changes
- Rerun viewers may control the topics that are sent over the logging API
- Rerun viewers may change the way topics are logged by the agent by specifying visualization parameters (similar to Rviz or Foxglove)
- Support a local file-based sink on the agent to capture a superset of a different set altogether of the topics sent to Rerun viewers
