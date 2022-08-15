use std::{convert::Infallible, io::Error as IoError};

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Unexpected Infallible error")]
    Infallible(#[from] Infallible),
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectorLoadError {
    #[error("Invalid yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("ByteSize: {0}")]
    ByteSizeParse(String),
}
