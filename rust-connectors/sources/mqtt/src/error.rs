use anyhow::Error as AnyhowError;
use fluvio::FluvioError;
use rumqttc::Error as MqttError;
use rumqttc::{ClientError as MqttClientError, ConnectionError as MqttConnectionError};
use serde_json::Error as SerdeJsonError;
use structopt::clap::Error as ClapError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MqttConnectorError {
    #[error("Mqtt error: `{0:#?}`.")]
    Mqtt(MqttError),
    #[error("Mqtt connecton error: `{0:#?}`.")]
    MqttConnection(#[from] MqttConnectionError),
    #[error("Mqtt client error: `{0:#?}`.")]
    MqttClient(#[from] MqttClientError),
    #[error("Fluvio error: `{0:?}`.")]
    Fluvio(#[from] FluvioError),
    #[error("Clap Error: `{0:#?}`.")]
    Clap(#[from] ClapError),
    #[error("Serde Json Error: `{0:#?}`.")]
    SerdeJson(#[from] SerdeJsonError),
    #[error("Anyhow Error: `{0:#?}`.")]
    Anyhow(#[from] AnyhowError),
}

impl From<MqttError> for MqttConnectorError {
    fn from(e: MqttError) -> Self {
        Self::Mqtt(e)
    }
}
