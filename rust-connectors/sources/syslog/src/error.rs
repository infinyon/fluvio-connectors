use crate::config::ConfigError;
use thiserror::Error;

pub use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Anyhow Error {0}")]
    Anyhow(#[from] AnyError),

    #[error("Fluvio Error {0}")]
    Fluvio(#[from] fluvio::FluvioError),

    #[error("Dataplane error: {0}")]
    DataPlaneError(#[from] fluvio::dataplane::link::ErrorCode),

    #[error("Io Error {0}")]
    Io(#[from] std::io::Error),

    #[error("Notify Error {0}")]
    Notify(#[from] notify::Error),

    #[error("Config Error {0}")]
    Config(#[from] ConfigError),

    #[error("Other error {0}")]
    Other(String),
}
