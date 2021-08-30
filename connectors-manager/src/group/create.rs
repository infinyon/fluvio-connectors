//!
//! # Create a Managed Connector
//!
//! CLI tree to generate Create a Managed Connector
//!

use fluvio_controlplane_metadata::managed_connector::{
    ManagedConnectorConfig, ManagedConnectorSpec,
};
use structopt::StructOpt;
use tracing::debug;

use fluvio::Fluvio;

use crate::error::ConnectorError as ClusterCliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateManagedConnectorOpt {
    /// The name for the new Managed Connector
    #[structopt(value_name = "name")]
    pub name: String,
}

impl CreateManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ClusterCliError> {
        // let (name, spec) = self.validate();

        let name = self.name.clone();
        let spec = ManagedConnectorSpec {
            name: name.clone(),
            config: ManagedConnectorConfig {
                r#type: "type1".to_owned(),
                topic: "type1topic".to_owned(),
                args: todo!(),
            },
        };

        debug!("creating managed_connector: {}, spec: {:#?}", name, spec);

        let admin = fluvio.admin().await;
        admin.create(name, false, spec).await?;

        Ok(())
    }
}
