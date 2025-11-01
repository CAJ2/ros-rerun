use std::{path::PathBuf, sync::Arc};

use log::{debug, error};
use rclrs::DynamicSubscription;
use stream_cancel::Tripwire;

use crate::{
    archetypes::{
        archetype::{ArchetypeConverter, ConverterRegistry, FindConverterResult},
        ROSTypeName,
    },
    channel::{ArchetypeReceiver, ArchetypeSender, LogData},
    config::{DBConfig, StreamConfig, TopicSource},
};

pub struct SubscriptionWorker {
    topic: String,
    subscription: DynamicSubscription,
    converter: Arc<Box<dyn ArchetypeConverter>>,
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
        let archetype_name = rerun::ArchetypeName::from(config.archetype.as_str());
        // TODO: Handle message type auto-discovery
        let valid_ros_type = config
            .ros_type
            .as_ref()
            .expect("ROS type auto-discovery is not yet implemented");
        let ros_type: ROSTypeName = valid_ros_type.as_str().try_into()?;

        // Find the converter for the given archetype and ROS type
        // falling back to a more generic converter if needed
        let found_converter = registry.find_converter(archetype_name, &ros_type);
        let mut found_converter = match found_converter {
            FindConverterResult::Components(converter)
            | FindConverterResult::ArchetypeCustom(converter)
            | FindConverterResult::ArchetypeROSType(converter) => converter,
            FindConverterResult::NotFound(err) => {
                return Err(err);
            }
        };
        found_converter.set_config(&config.topic, &ros_type, config.converter.clone())?;
        let converter = Arc::new(found_converter);
        let cb_converter = converter.clone();
        debug!(
            "Creating subscription to topic '{}' with ROS type '{}' and archetype '{}'",
            config.topic, ros_type, archetype_name
        );

        let sub = node.create_dynamic_subscription(
            ros_type.into(),
            config.topic.as_str(),
            move |msg: rclrs::DynamicMessage, _info: rclrs::MessageInfo| {
                let instance = cb_converter.clone();
                let channel = channel.clone();
                tokio::spawn(async move {
                    for tx in channel.tx {
                        if let Ok(arch_msg) = instance.convert(msg.view()).await {
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
            subscription: sub,
            converter,
        })
    }
}

pub struct GRPCSinkWorker {
    address: String,
    rec: rerun::RecordingStream,
}

impl GRPCSinkWorker {
    pub fn new(config: &StreamConfig) -> anyhow::Result<Self> {
        let rec = rerun::RecordingStreamBuilder::new("rerun_ros")
            .connect_grpc_opts(config.url.clone(), rerun::default_flush_timeout())?;

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
        self.rec.flush_blocking();
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
                        rec_stream.log(arch.entity_path.as_ref(), &arch.components.as_serialized_batches());
                    }
                    LogData::ArchetypeArray(archs) => {
                        for arch in archs {
                            rec_stream.log(arch.entity_path.as_ref(), &arch.components.as_serialized_batches());
                        }
                    },
                    LogData::AnyComponents(comps) => {
                        rec_stream.log(comps.entity_path.as_ref(), &comps.components.as_serialized_batches());
                    },
                    LogData::AnyComponentsArray(comps) => {
                        for comps in comps {
                            rec_stream.log(comps.entity_path.as_ref(), &comps.components.as_serialized_batches());
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
    file: PathBuf,
    rec: rerun::RecordingStream,
}

impl DBSinkWorker {
    pub fn new(config: &DBConfig) -> anyhow::Result<Self> {
        let store_id = rerun::StoreId::random(rerun::StoreKind::Recording);
        let file_name = format!("{store_id}.rrd");
        let recording_file = config.data_dir.clone().join(file_name);
        let rec = rerun::RecordingStreamBuilder::new("rerun_ros")
            .store_id(store_id)
            .save(recording_file.clone())?;

        Ok(Self {
            file: recording_file,
            rec,
        })
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
                        rec_stream.log(arch.entity_path.as_ref(), &arch.components.as_serialized_batches());
                    }
                    LogData::ArchetypeArray(archs) => {
                        for arch in archs {
                            rec_stream.log(arch.entity_path.as_ref(), &arch.components.as_serialized_batches());
                        }
                    },
                    LogData::AnyComponents(comps) => {
                        rec_stream.log(comps.entity_path.as_ref(), &comps.components.as_serialized_batches());
                    },
                    LogData::AnyComponentsArray(comps) => {
                        for comps in comps {
                            rec_stream.log(comps.entity_path.as_ref(), &comps.components.as_serialized_batches());
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
