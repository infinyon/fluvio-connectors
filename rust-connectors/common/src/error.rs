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
