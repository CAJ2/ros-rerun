use anyhow::Result;
use log::{error, info};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::{fs, io};
use thiserror::Error;
use toml::de::Error as TomlError;
use toml::ser::Error as TomlSeError;

pub mod defs;

use crate::cli::Options;
use crate::config::defs::{Api, Config};

pub static CONFIG: std::sync::LazyLock<RwLock<Config>> = std::sync::LazyLock::new(RwLock::default);

/// Errors occurring during config loading.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("failed to read config file")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Toml(#[from] TomlError),

    #[error(transparent)]
    TomlSe(#[from] TomlSeError),
}

/// Load the configuration file.
pub fn load(options: &Options) {
    let config_path = options
        .config_file
        .clone()
        .or_else(|| Some(PathBuf::from("config.toml")));

    // Load the config using the following fallback behavior:
    //  - Config path + CLI overrides
    //  - CLI overrides
    //  - Default
    config_path
        .as_ref()
        .and_then(|config_path| load_from(config_path).ok())
        .unwrap_or_else(|| {
            let mut config = CONFIG.write();
            match config_path {
                Some(config_path) => config.config_paths.push(config_path),
                None => info!("No config file found; using default"),
            };
        });

    after_loading(options);
}

/// Modifications after the `Config` object is created.
fn after_loading(options: &Options) {
    // Override config with CLI options.
    options.override_config(&mut CONFIG.write());
}

/// Load configuration file and log errors.
fn load_from(path: &Path) -> Result<(), ConfigError> {
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

    // Load configuration file as Value.
    let mut config: Config = toml::from_str(&contents)?;
    config.config_paths.push(path.to_path_buf());

    Ok(config)
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
    fn messages_config() {
        let config: Config = toml::from_str(
            r#"
            [messages.example_msg]
            topic = "example_topic"
            ros_type = "std_msgs/String"
            archetype = "TextLog"
            "#,
        )
        .unwrap();

        assert_eq!(config.messages().count(), 1);
        let (name, msg) = config.messages().next().unwrap();
        assert_eq!(name, "example_msg");
        assert_eq!(msg.topic(), "example_topic");
        assert_eq!(msg.ros_type(), "std_msgs/String");
        assert_eq!(msg.archetype(), "TextLog");
    }
}
