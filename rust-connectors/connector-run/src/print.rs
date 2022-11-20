use clap::Parser;
use fluvio_connectors_common::config::ConnectorConfig;

use crate::convert_to_k8_deployment;

#[derive(Debug, Parser)]
pub struct PrintOpt {
    /// path to the connector config
    #[clap(short = 'c', long)]
    config: String,
}

impl PrintOpt {
    pub fn execute(self) -> anyhow::Result<()> {
        let config = ConnectorConfig::from_file(self.config)?;
        let deployment = convert_to_k8_deployment(&config)?;
        println!("{}", serde_yaml::to_string(&deployment)?);
        Ok(())
    }
}
