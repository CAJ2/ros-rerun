use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top level configuration
///
/// Any changes to the configuration will eventually be reflected
/// in the topology, but this process happens asynchronously
/// to allow pending logs to flush.
#[derive(Deserialize, Serialize, Default, Clone, Debug, PartialEq)]
pub struct Config {
    /// GRPC server configuration
    #[serde(default)]
    pub api: Api,

    /// ROS topics configuration
    #[serde(default)]
    pub topics: HashMap<String, TopicSource>,

    /// Rerun SDK streams configuration
    /// The bridge will log messages over gRPC directly
    #[serde(default)]
    pub streams: HashMap<String, StreamConfig>,

    #[serde(default)]
    pub db: DBConfig,

    /// Path where config was loaded from.
    #[serde(skip)]
    pub config_paths: Vec<PathBuf>,
}

impl Config {
    pub fn topics(&self) -> impl IntoIterator<Item = (&String, &TopicSource)> {
        self.topics.iter().collect::<Vec<_>>()
    }

    pub fn streams(&self) -> impl IntoIterator<Item = (&String, &StreamConfig)> {
        self.streams.iter().collect::<Vec<_>>()
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Api {
    pub enabled: bool,
    pub address: std::net::SocketAddr,
}

impl Default for Api {
    fn default() -> Self {
        let address = "127.0.0.1:9888".parse().expect("Invalid address");
        Self {
            enabled: true,
            address,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, PartialEq)]
pub struct TopicSource {
    pub topic: String,
    pub ros_type: Option<String>,
    pub archetype: String,

    /// Additional settings for the converter
    #[serde(flatten)]
    pub converter: ConverterSettings,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, PartialEq)]
pub struct ConverterSettings(toml::Table);

impl ConverterSettings {
    pub fn get(&self, key: &str) -> Option<&toml::Value> {
        self.0.get(key)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    pub inputs: Vec<String>,
    pub url: String,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct DBConfig {
    pub data_dir: PathBuf,
    pub inputs: Vec<String>,
}
