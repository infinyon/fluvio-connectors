use anyhow::Error as AnyhowError;
use fluvio::FluvioError;
use paho_mqtt::Error as MqttError;
use structopt::clap::Error as ClapError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MqttConnectorError {
    #[error("Mqtt error: `{0:?}`.")]
    Mqtt(#[from] MqttError),
    #[error("Fluvio error: `{0:?}`.")]
    Fluvio(#[from] FluvioError),
    #[error("Clap Error: `{0:#?}`.")]
    Clap(#[from] ClapError),
    #[error("Anyhow Error: `{0:#?}`.")]
    Anyhow(#[from] AnyhowError),
}
