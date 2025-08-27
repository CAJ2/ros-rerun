use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::{collections::HashMap, net::IpAddr};

use serde::{Deserialize, Serialize};

/// Top level configuration
///
/// Any changes to the configuration will eventually be reflected
/// in the topology, but this process happens asynchronously
/// to allow pending logs to flush.
#[derive(Deserialize, Serialize, Default, Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// GRPC server configuration
    #[serde(default)]
    pub api: Api,

    /// ROS messages configuration
    #[serde(default)]
    pub messages: HashMap<String, MessageSource>,

    /// Rerun SDK streams configuration
    #[serde(default)]
    pub streams: HashMap<String, StreamConfig>,

    /// Path where config was loaded from.
    #[serde(skip)]
    pub config_paths: Vec<PathBuf>,
}

impl Config {
    pub fn messages(&self) -> impl Iterator<Item = (&String, &MessageSource)> {
        self.messages.iter()
    }

    pub fn config_paths(&self) -> impl Iterator<Item = &PathBuf> {
        self.config_paths.iter()
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Api {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub address: String,
}

impl Default for Api {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "127.0.0.1:9888".into(),
        }
    }
}

impl Api {
    pub fn address(&self) -> std::net::SocketAddr {
        self.address.parse().unwrap_or_else(|_| {
            println!("Failed to parse API address, using default");
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9888)
        })
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct MessageSource {
    pub topic: String,
    pub ros_type: String,
    pub archetype: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    pub inputs: Vec<String>,
    pub url: String,
}
