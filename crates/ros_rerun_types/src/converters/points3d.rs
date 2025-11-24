use std::sync::Arc;

use async_trait::async_trait;
use rerun::Archetype as _;
use serde::{Deserialize, Serialize};

use crate::{
    converter::{Converter, ConverterCfg, ConverterError, ConverterSettings, LogPacket},
    converters::deserialize_view,
    definitions::sensor_msgs::PointCloud2,
    parsers::sensor_msgs::Position3DIter,
    ROSTypeString, RerunName,
};

const SENSOR_MSGS_POINTCLOUD2: ROSTypeString<'_> = ROSTypeString("sensor_msgs", "PointCloud2");

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct PointCloudConfig {
    color: Option<String>,
}

impl PointCloudConfig {
    fn parse(
        &mut self,
        config: &ConverterSettings,
        rerun_name: RerunName,
        ros_type: &ROSTypeString<'_>,
    ) -> anyhow::Result<(), ConverterError> {
        if let Some(color) = config.0.get("color") {
            let color_str = color.as_str().ok_or(ConverterError::InvalidConfig(
                rerun_name,
                ros_type.to_string(),
                anyhow::anyhow!("'color' must be a string"),
            ))?;
            self.color = Some(color_str.to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct SensorPointCloud2ToPoints3D {}

impl ConverterCfg for SensorPointCloud2ToPoints3D {
    fn set_config(&mut self, config: ConverterSettings) -> anyhow::Result<(), ConverterError> {
        if !config.0.is_empty() {
            Err(ConverterError::InvalidConfig(
                self.rerun_name(),
                SENSOR_MSGS_POINTCLOUD2.to_string(),
                anyhow::anyhow!("SensorPointCloud2ToPoints3D does not accept any configuration"),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Converter for SensorPointCloud2ToPoints3D {
    fn rerun_name(&self) -> RerunName {
        RerunName::RerunArchetype(rerun::Points3D::name())
    }

    fn ros_type(&self) -> Option<&ROSTypeString<'static>> {
        Some(&SENSOR_MSGS_POINTCLOUD2)
    }

    async fn convert_view<'a>(
        &self,
        msg: rclrs::DynamicMessageView<'a>,
    ) -> anyhow::Result<LogPacket, ConverterError> {
        let point_cloud = deserialize_view::<PointCloud2>(msg)?;
        let pos_iter = Position3DIter::try_new(
            &point_cloud.data,
            point_cloud.point_step as usize,
            point_cloud.is_bigendian,
            &point_cloud.fields,
        )
        .ok_or_else(|| {
            ConverterError::Conversion(
                self.rerun_name(),
                SENSOR_MSGS_POINTCLOUD2.to_string(),
                anyhow::anyhow!("failed to create Position3D iterator"),
            )
        })?;
        let positions = pos_iter.collect::<Vec<_>>();
        let points3d = rerun::Points3D::new(positions);
        Ok(LogPacket::new(points3d))
    }
}
