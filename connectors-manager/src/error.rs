use thiserror::Error;

// TODO: Add more error types
#[derive(Debug, Error)]
pub enum ConnectorError {}

use fluvio::FluvioError;
impl From<FluvioError> for ConnectorError {
    fn from(err: FluvioError) -> Self {
        println!("{:?}", err);
        todo!();
    }
}
use fluvio_extension_common::output::OutputError;
impl From<OutputError> for ConnectorError {
    fn from(err: OutputError) -> Self {
        println!("{:?}", err);
        todo!();
    }
}
