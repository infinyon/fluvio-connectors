use crate::error::ConnectorError;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub struct ListOpts {
    config: Option<String>,
}

impl ListOpts {
    pub async fn exec(&self) -> Result<(), ConnectorError> {
        //TODO:
        // * Determine the current fluvio cluster config.
        // * If local, look for all the processes in the config
        // * If k8s, look all the pods and see if the connector is actually still running.
        todo!();
    }
}
