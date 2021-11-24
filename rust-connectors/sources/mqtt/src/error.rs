use anyhow::Error as AnyhowError;
use fluvio::FluvioError;
//use paho_mqtt::Error as MqttError;
use rumqttc::Error as MqttError;
use rumqttc::ConnectionError as MqttConnectionError;
use rumqttc::ClientError  as MqttClientError;
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
    #[error("Anyhow Error: `{0:#?}`.")]
    Anyhow(#[from] AnyhowError),
}

impl From<MqttError> for MqttConnectorError {
    fn from(e: MqttError) -> Self {
        Self::Mqtt(e)
    }
}
