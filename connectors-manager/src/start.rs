use crate::error::ConnectorError;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct StartOpts {
    config: String,
}

impl StartOpts {
    pub async fn exec(&self) -> Result<(), ConnectorError> {
        //TODO:
        // * Determine the current fluvio cluster config.
        // * If local, spin up the executable.
        // * If k8s, do the k8s setup to run the connector and then run it.
        todo!();
    }
}
