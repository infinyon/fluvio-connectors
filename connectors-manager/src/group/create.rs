//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
//!

use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::spg::*;

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
        let (name, spec) = self.validate();
        debug!("creating spg: {}, spec: {:#?}", name, spec);

        let admin = fluvio.admin().await;
        admin.create(name, false, spec).await?;

        Ok(())
    }

    /// Validate cli options. Generate target-server and create spu group config.
    fn validate(self) -> (String, SpuGroupSpec) {
        todo!();
        /*
        let storage = self.storage_size.map(|storage_size| StorageConfig {
            size: Some(storage_size),
            ..Default::default()
        });

        let spu_config = SpuConfig {
            storage,
            rack: self.rack,
            ..Default::default()
        };

        let spec = SpuGroupSpec {
            replicas: self.replicas,
            min_id: self.min_id,
            spu_config,
        };
        (self.name, spec)
        */
    }
}
