use crate::converter::ConverterError;

pub mod points3d;
pub mod text;

pub(crate) fn deserialize_view<'de, T>(
    msg: rclrs::DynamicMessageView<'de>,
) -> anyhow::Result<T, ConverterError>
where
    T: serde::de::Deserialize<'de>,
{
    rclrs::dynamic_message::from_dyn_msg_view::<'de, T>(msg)
        .map_err(|err| ConverterError::Deserialization(err))
}
