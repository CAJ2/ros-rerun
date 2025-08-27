use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueHint};
use log::LevelFilter;

use crate::config::defs::Config;

/// CLI options for the Rerun ROS executable.
#[derive(Parser, Debug)]
#[clap(author, about, version = env!("VERSION"))]
pub struct Options {
    /// Path to the configuration file
    #[arg(short, long, value_name = "FILE", value_hint = ValueHint::FilePath)]
    pub config: Option<PathBuf>,

    /// Set the log level
    #[arg(long, default_value_t = LevelFilter::Info)]
    pub log_level: LevelFilter,

    /// GRPC server listen address
    #[arg(long)]
    pub listen: Option<String>,

    /// Subcommand passed to the CLI.
    #[command(subcommand)]
    pub subcommands: Option<Subcommands>,
}

impl Options {
    pub fn new() -> Self {
        Self::parse()
    }

    pub fn override_config(&self, config: &mut Config) {
        // Override listen address if specified
        if let Some(listen) = &self.listen {
            config.api.address = listen.clone();
        }
    }
}

/// Available CLI subcommands.
#[derive(Subcommand, Debug)]
pub enum Subcommands {
    Configure(ConfigureOptions),
}

#[derive(Args, Debug)]
pub struct ConfigureOptions {
    #[arg(short, long, value_name = "FILE", value_hint = ValueHint::FilePath)]
    pub config: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use crate::config::CONFIG;

    use super::*;

    #[test]
    fn cli_override_config() {
        let opts = Options {
            config: Some(PathBuf::from("config.toml")),
            log_level: LevelFilter::Debug,
            listen: Some("1.1.1.1:9001".into()),
            subcommands: None,
        };
        opts.override_config(&mut CONFIG.write());
        let config = CONFIG.read();
        assert_eq!(config.api.address(), "1.1.1.1:9001".parse().unwrap());
    }
}
