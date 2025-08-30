use log::{error, info};
use rclrs::{CreateBasicExecutor as _, InitOptions, RclrsErrorFilter as _, SpinOptions};
use rerun_ros::{
    cli::{Options, Subcommands},
    config,
    node::NodeGraph,
};
use std::env;

fn main() -> anyhow::Result<()> {
    let options = Options::new();

    // Initialize logging
    env_logger::Builder::new()
        .filter_level(options.log_level)
        .init();

    config::load(&options)?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    match options.subcommands {
        Some(Subcommands::Configure(configure_options)) => {
            info!("Configuring with options: {configure_options:?}");
        }
        None => rt.block_on(run())?,
    }

    Ok(())
}

async fn run() -> anyhow::Result<()> {
    info!("Starting Rerun ROS bridge...");

    let context = rclrs::Context::new(env::args(), InitOptions::new())?;
    let mut executor = context.create_basic_executor();
    let graph = NodeGraph::new(&executor)?;
    tokio::spawn(async move {
        graph.run().await;
    });

    info!("Bridge is running. Press Ctrl+C to exit.");
    tokio::task::block_in_place(|| {
        if let Err(e) = executor.spin(SpinOptions::default()).first_error() {
            error!("Executor spin error: {e:?}");
        }
    });

    Ok(())
}
