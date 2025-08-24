use log::{debug, info};
use rclrs::{CreateBasicExecutor as _, InitOptions, RclrsErrorFilter as _, SpinOptions, Value};
use std::env;

use crate::{
    cli::{Options, Subcommands},
    config::CONFIG,
};

mod cli;
mod config;

fn main() -> anyhow::Result<()> {
    let options = Options::new();

    // Initialize logging
    env_logger::Builder::new()
        .filter_level(options.log_level)
        .init();

    config::load(&options);

    match options.subcommands {
        Some(Subcommands::Configure(configure_options)) => {
            println!("Configuring with options: {:?}", configure_options);
        }
        None => run()?,
    }

    Ok(())
}

fn run() -> anyhow::Result<()> {
    info!("Starting Rerun ROS bridge...");

    let context = rclrs::Context::new(env::args(), InitOptions::new())?;
    let mut executor = context.create_basic_executor();
    let node = executor.create_node("rerun_ros_bridge")?;
    let worker = node.create_worker::<usize>(0);
    let cfg = CONFIG.read();
    let config_entries: Vec<_> = cfg.messages().collect();

    // Prevent the subscriptions from being dropped
    let mut _subscriptions = Vec::new();
    for (_name, msg) in config_entries {
        let (topic, ros_type) = (msg.topic(), msg.ros_type());
        info!("Subscribing to topic: {topic} with type: {ros_type}");
        let _msg_spec = rerun_ros::ros_introspection::MsgSpec::new(ros_type)?;

        let sub = worker.create_dynamic_subscription(
            ros_type.try_into()?,
            topic,
            move |num: &mut usize, msg, _msg_info| {
                *num += 1;
                println!("#{} | I heard: '{:#?}'", *num, msg.structure());
                msg.structure().fields.iter().for_each(|f| {
                    if let Some(v) = msg.get(f.name.as_str()) {
                        println!("  - {}: {:?}", f.name, v);
                    }
                });
            },
        )?;
        _subscriptions.push(sub);
    }

    info!("Bridge is running. Press Ctrl+C to exit.");
    executor.spin(SpinOptions::default()).first_error()?;

    Ok(())
}
