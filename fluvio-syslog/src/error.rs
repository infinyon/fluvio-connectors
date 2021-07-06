use thiserror::Error;


#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Fluvio Error {0}")]
    FluvioError(#[from] fluvio::FluvioError),

    #[error("Io Error {0}")]
    IoError(#[from] std::io::Error),

    #[error("Notify Error {0}")]
    NotifyError(#[from] notify::Error),
}
