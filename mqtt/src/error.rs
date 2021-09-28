use thiserror::Error;
use paho_mqtt::Error as MqttError;
use fluvio::FluvioError;

#[derive(Error, Debug)]
pub enum MqttConnectorError {
    #[error("Mqtt error: `{0:?}`.")]
    Mqtt(#[from] MqttError),
    #[error("Fluvio error: `{0:?}`.")]
    Fluvio(#[from] FluvioError),
}
