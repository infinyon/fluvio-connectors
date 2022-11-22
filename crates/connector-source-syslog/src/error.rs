use crate::config::ConfigError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectorError {
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
}
