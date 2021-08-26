//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
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
pub struct CreateManagedSpuGroupOpt {
    /// The name for the new SPU Group
    #[structopt(value_name = "name")]
    pub name: String,
}

impl CreateManagedSpuGroupOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ClusterCliError> {
        // let (name, spec) = self.validate();

        let name = self.name.clone();
        let spec = ManagedConnectorSpec {
            name: name.clone(),
            config: ManagedConnectorConfig {
                r#type: "type1".to_owned(),
                topic: "type1topic".to_owned(),
            },
        };

        debug!("creating managed_connector: {}, spec: {:#?}", name, spec);

        let admin = fluvio.admin().await;
        //fluvio_future::timer::sleep(std::time::Duration::from_secs(5)).await;
        admin.create(name, false, spec).await?;
        /*
        admin.create(
            name,
            false,
            fluvio_controlplane_metadata::topic::TopicSpec::default()
        ).await?;
        */

        Ok(())
    }
}
