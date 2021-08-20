use crate::error::ConnectorError;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct StopOpts {
    /// If config is not here, stop all connectors.
    config: Option<String>,
}

impl StopOpts {
    pub async fn exec(&self) -> Result<(), ConnectorError> {
        // TODO: Similar to start but stop instead
        // * Determine the current fluvio cluster config.
        // * If local, run `pkill` on the executable.
        // * If k8s, do the k8s setup, stop the container for this connector
        todo!();
    }
}
