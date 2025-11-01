use anyhow::Result;
use log::{error, info};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::{fs, io};
use thiserror::Error;
use toml::de::Error as TomlError;
use toml::ser::Error as TomlSeError;

pub mod defs;
pub use defs::{Api, Config, DBConfig, StreamConfig, TopicSource};

use crate::cli::Options;

pub static CONFIG: std::sync::LazyLock<RwLock<Config>> = std::sync::LazyLock::new(RwLock::default);

/// Errors occurring during config loading.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("failed to find config file")]
    NotFound,

    #[error("failed to validate config")]
    Validation(#[from] anyhow::Error),

    #[error("failed to read config file")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Toml(#[from] TomlError),

    #[error(transparent)]
    TomlSe(#[from] TomlSeError),
}

/// Load the configuration file
///
/// The configuration must be a TOML file.
/// The search order is as follows:
/// 1. CLI --config argument file path
/// 2. config.toml in the current directory
pub fn load(options: &Options) -> Result<(), ConfigError> {
    let config_path = options.config.clone().filter(|p| p.is_file()).or_else(|| {
        let path = PathBuf::from("config.toml");
        if path.is_file() {
            Some(path)
        } else {
            None
        }
    });

    match config_path {
        Some(path) => load_from_path(&path).map(|_| {
            let mut config = CONFIG.write();
            config.config_paths.push(path);

            // Modifications after the `Config` object is created.
            options.override_config(&mut config);
        }),
        None => Err(ConfigError::NotFound),
    }
}

/// Load configuration file and log errors.
fn load_from_path(path: &Path) -> Result<(), ConfigError> {
    match read_config(path) {
        Ok(loaded_config) => {
            let mut config = CONFIG.write();
            *config = loaded_config;
            Ok(())
        }
        Err(ConfigError::Io(io)) if io.kind() == io::ErrorKind::NotFound => {
            error!("Unable to load config {path:?}: File not found");
            Err(ConfigError::Io(io))
        }
        Err(err) => {
            error!("Unable to load config {path:?}: {err}");
            Err(err)
        }
    }
}

/// Read configuration file from path.
fn read_config(path: &Path) -> Result<Config, ConfigError> {
    let contents = fs::read_to_string(path)?;

    let mut config: Config = toml::from_str(&contents)?;
    config.config_paths.push(path.to_path_buf());

    validate_config(&config)?;

    Ok(config)
}

fn validate_config(config: &Config) -> Result<(), ConfigError> {
    config.db.validate()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config() {
        toml::from_str::<Config>("").unwrap();
    }

    #[test]
    fn default_config() {
        let config: Config = toml::from_str("").unwrap();
        assert!(config.api.enabled);
        assert_eq!(config.api.address, Api::default().address);
    }

    #[test]
    fn topics_config() {
        let config: Config = toml::from_str(
            r#"
            [topics.example_msg]
            topic = "example_topic"
            ros_type = "std_msgs/String"
            archetype = "TextLog"
            "#,
        )
        .unwrap();

        assert_eq!(config.topics.len(), 1);
        let (name, topic) = config.topics.iter().next().unwrap();
        assert_eq!(name, "example_msg");
        assert_eq!(topic.topic, "example_topic");
        assert_eq!(topic.ros_type, Some("std_msgs/String".into()));
        assert_eq!(topic.archetype, "TextLog");
    }

    #[test]
    fn topics_settings_config() {
        let config: Config = toml::from_str(
            r#"
            [topics.example_msg]
            topic = "example_topic"
            archetype = "TextLog"
            field = "example_field"
            another_setting = "example_value"
            "#,
        )
        .unwrap();

        assert_eq!(config.topics.len(), 1);
        let (name, topic) = config.topics.iter().next().unwrap();
        assert_eq!(name, "example_msg");
        assert_eq!(topic.topic, "example_topic");
        assert_eq!(topic.ros_type, None);
        assert_eq!(topic.archetype, "TextLog");
        assert_eq!(
            topic.converter.get("field"),
            Some(&toml::Value::String("example_field".into()))
        );
        assert_eq!(
            topic.converter.get("another_setting"),
            Some(&toml::Value::String("example_value".into()))
        );
    }
}
