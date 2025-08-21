use anyhow::{Error, Result};
use clap::Parser;
use rclrs::{CreateBasicExecutor, InitOptions, RclrsErrorFilter, SpinOptions};
use rerun_ros::config::ConfigParser;
use std::env;
use std::sync::Arc;

/// A bridge between rerun and ROS
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct BridgeArgs {
    /// Path to the configuration file in TOML format
    #[arg(short, long)]
    config_file: String,
}

fn main() -> Result<(), Error> {
    let bridge_args = BridgeArgs::parse();

    if bridge_args.config_file.is_empty() {
        return Ok(());
    }

    println!("Starting bridge");
    let config_parser = ConfigParser::new(&bridge_args.config_file)?;

    let context = rclrs::Context::new(env::args(), InitOptions::new())?;
    let mut executor = context.create_basic_executor();
    let node = executor.create_node("rerun_ros_bridge")?;
    let worker = node.create_worker::<usize>(0);
    // Clippy does not like iterating over the keys of a HashMap, so we collect it into a Vec
    let config_entries: Vec<_> = config_parser.conversions().iter().collect();

    // Prevent the subscriptions from being dropped
    let mut _subscriptions = Vec::new();
    for ((topic_name, _frame_id), (ros_type, _entity_path)) in config_entries {
        let _msg_spec = rerun_ros::ros_introspection::MsgSpec::new(ros_type)?;

        println!("Subscribing to topic: {topic_name} with type: {ros_type}");
        let sub = worker.create_dynamic_subscription(
            ros_type.as_str().try_into()?,
            topic_name,
            move |num: &mut usize, msg, _msg_info| {
                *num += 1;
                println!("#{} | I heard: '{:#?}'", *num, msg.structure());
            },
        )?;
        _subscriptions.push(sub);
    }

    println!("Bridge is running. Press Ctrl+C to exit.");
    executor.spin(SpinOptions::default()).first_error()?;
    Ok(())
}
