use std::sync::Arc;

use log::{debug, error};
use rclrs::DynamicSubscription;
use ros_rerun_types::{
    converter::{Converter, ConverterBuilder, ConverterRegistry, ConverterSettings},
    ROSTypeName, RerunName,
};
use stream_cancel::Tripwire;

use crate::{
    channel::{ArchetypeReceiver, ArchetypeSender, LogComponents, LogData},
    config::{DBConfig, StreamConfig, TopicSource},
};

pub struct SubscriptionWorker {
    topic: String,
    _subscription: DynamicSubscription,
    _converter: Arc<Box<dyn Converter>>,
}

impl SubscriptionWorker {
    /// Create a new subscription worker.
    ///
    /// This will create a new subscription to the specified ROS topic and
    /// set up the necessary message transformation.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription cannot be created.
    pub fn new(
        node: &rclrs::Node,
        config: &TopicSource,
        registry: &ConverterRegistry,
        channel: ArchetypeSender,
    ) -> anyhow::Result<Self> {
        let rerun_name =
            RerunName::RerunArchetype(rerun::ArchetypeName::from(config.archetype.as_str()));
        // TODO: Handle message type auto-discovery
        let valid_ros_type = config
            .ros_type
            .as_ref()
            .expect("ROS type auto-discovery is not yet implemented");
        let ros_type: ROSTypeName = valid_ros_type.as_str().try_into()?;

        let converter = ConverterBuilder::new_with_registry(registry)
            .topic(&config.topic)
            .ros_type(ros_type.clone())
            .rerun_name(rerun_name.clone())
            .config(ConverterSettings(config.converter.clone()))
            .build()?;
        let converter = Arc::new(converter);
        let cb_converter = converter.clone();
        let topic = Arc::new(config.topic.clone());
        debug!(
            "Creating subscription to topic '{}' with ROS type '{}' and archetype '{}'",
            config.topic, ros_type, rerun_name,
        );

        let sub = node.create_dynamic_subscription(
            ros_type.into(),
            config.topic.as_str(),
            move |msg: rclrs::DynamicMessage, _info: rclrs::MessageInfo| {
                let instance = cb_converter.clone();
                let channel = channel.clone();
                let topic = topic.clone();
                tokio::spawn(async move {
                    for tx in channel.tx {
                        if let Ok(convert_data) = instance.convert_view(msg.view()).await {
                            let arch_msg = LogData::Archetype(LogComponents {
                                entity_path: topic.clone(),
                                header: convert_data.header,
                                components: convert_data.components,
                            });
                            if let Err(err) = tx.send(arch_msg) {
                                error!("Failed to send archetype data: {err:?}");
                            }
                        }
                    }
                });
            },
        )?;

        Ok(Self {
            topic: config.topic.clone(),
            _subscription: sub,
            _converter: converter,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

pub struct GRPCSinkWorker {
    address: String,
    rec: rerun::RecordingStream,
}

impl GRPCSinkWorker {
    /// Create a worker that sends data to a gRPC Rerun server.
    ///
    /// # Errors
    /// Returns an error if the connection to the gRPC server cannot be established.
    pub fn new(config: &StreamConfig) -> anyhow::Result<Self> {
        let rec = rerun::RecordingStreamBuilder::new("ros_rerun")
            .connect_grpc_opts(config.url.clone())?;

        Ok(Self {
            address: config.url.clone(),
            rec,
        })
    }

    pub fn run(&self, channel: ArchetypeReceiver, shutdown: Tripwire) {
        let shared_rec = self.rec.clone();
        tokio::spawn(run_grpc_sink_worker(shared_rec, channel, shutdown));
    }
}

impl Drop for GRPCSinkWorker {
    fn drop(&mut self) {
        debug!("Shutting down gRPC sink to {}", self.address);
        if let Err(err) = self.rec.flush_blocking() {
            error!("Failed to flush gRPC recording stream: {err}");
        }
    }
}

fn send_log_comps(rec_stream: &rerun::RecordingStream, data: &LogComponents) {
    if let Err(err) = rec_stream.log(
        data.entity_path.as_str(),
        &data.components.as_serialized_batches(),
    ) {
        error!("Failed to send log components: {err}");
    }
}

async fn run_grpc_sink_worker(
    rec_stream: rerun::RecordingStream,
    mut channel: ArchetypeReceiver,
    mut shutdown: Tripwire,
) {
    loop {
        tokio::select! {
            Some(log_data) = channel.rx.recv() => {
                match log_data {
                    LogData::Archetype(arch) => {
                        send_log_comps(&rec_stream, &arch);
                    }
                    LogData::ArchetypeArray(archs) => {
                        for arch in archs {
                            send_log_comps(&rec_stream, &arch);
                        }
                    },
                    LogData::AnyComponents(comps) => {
                        send_log_comps(&rec_stream, &comps);
                    },
                    LogData::AnyComponentsArray(comps_arr) => {
                        for comps in comps_arr {
                            send_log_comps(&rec_stream, &comps);
                        }
                    },
                }
            }
            _ = &mut shutdown => {
                debug!("Shutting down gRPC sink worker");
                break;
            }
        }
    }
}

pub struct DBSinkWorker {
    rec: rerun::RecordingStream,
}

impl DBSinkWorker {
    /// Create a worker that saves data to files in a local directory
    ///
    /// # Errors
    /// Returns an error if the recording stream cannot be created.
    pub fn new(config: &DBConfig) -> anyhow::Result<Self> {
        let store_id = rerun::StoreId::random(rerun::StoreKind::Recording, "ros_rerun");
        let file_name = format!("{}_{}.rrd", "ros_rerun", store_id.recording_id().as_str());
        let recording_file = config.data_dir.clone().join(file_name);
        let rec = rerun::RecordingStreamBuilder::new("ros_rerun")
            .recording_id(store_id.recording_id().clone())
            .save(recording_file.clone())?;

        Ok(Self { rec })
    }

    pub fn run(&self, channel: ArchetypeReceiver, shutdown: Tripwire) {
        let shared_rec = self.rec.clone();
        tokio::spawn(run_db_sink_worker(shared_rec, channel, shutdown));
    }
}

async fn run_db_sink_worker(
    rec_stream: rerun::RecordingStream,
    mut channel: ArchetypeReceiver,
    mut shutdown: Tripwire,
) {
    loop {
        tokio::select! {
            Some(log_data) = channel.rx.recv() => {
                match log_data {
                    LogData::Archetype(arch) => {
                        send_log_comps(&rec_stream, &arch);
                    }
                    LogData::ArchetypeArray(archs) => {
                        for arch in archs {
                            send_log_comps(&rec_stream, &arch);
                        }
                    },
                    LogData::AnyComponents(comps) => {
                        send_log_comps(&rec_stream, &comps);
                    },
                    LogData::AnyComponentsArray(comps) => {
                        for comps in comps {
                            send_log_comps(&rec_stream, &comps);
                        }
                    },
                }
            }
            _ = &mut shutdown => {
                debug!("Shutting down DB sink worker");
                break;
            }
        }
    }
}
