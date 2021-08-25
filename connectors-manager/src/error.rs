use thiserror::Error;
use fluvio_extension_common::output::OutputError;
use fluvio::FluvioError;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Fluvio Error `{0:?}`")]
    Fluvio(#[from] FluvioError),
    #[error("Otuput Error `{0:?}`")]
    Output(#[from] OutputError),
}
