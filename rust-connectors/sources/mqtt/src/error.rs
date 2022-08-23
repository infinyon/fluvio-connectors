use anyhow::Error as AnyhowError;

use fluvio_connectors_common::fluvio::FluvioError;

use clap::Error as ClapError;
use rumqttc::{ClientError as MqttClientError, ConnectionError as MqttConnectionError};
use rumqttc::{Error as MqttError, OptionError as MqttOptionError};
use serde_json::Error as SerdeJsonError;
use thiserror::Error;
use url::ParseError;

#[derive(Error, Debug)]
pub enum MqttConnectorError {
    #[error("Mqtt error: `{0}`.")]
    Mqtt(MqttError),
    #[error("Mqtt connecton error: `{0}`.")]
    MqttConnection(#[from] MqttConnectionError),
    #[error("Mqtt client error: `{0}`.")]
    MqttClient(#[from] MqttClientError),
    #[error("Mqtt OptionError: `{0}`.")]
    MqttOption(#[from] MqttOptionError),
    #[error("Fluvio error: `{0}`.")]
    Fluvio(#[from] FluvioError),
    #[error("Clap Error: `{0}`.")]
    Clap(#[from] ClapError),
    #[error("Serde Json Error: `{0}`.")]
    SerdeJson(#[from] SerdeJsonError),
    #[error("URL Parse Error: `{0}`.")]
    ParseError(#[from] ParseError),
    #[error("Anyhow Error: `{0:#?}`.")]
    Anyhow(#[from] AnyhowError),
}

impl From<MqttError> for MqttConnectorError {
    fn from(e: MqttError) -> Self {
        Self::Mqtt(e)
    }
}
