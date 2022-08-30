use clap::Parser;
use fluvio_connectors_common::config::ConnectorConfig;
use k8_client::{meta_client::MetadataClient, K8Config};
use k8_types::{app::deployment::DeploymentSpec, InputObjectMeta};

#[derive(Debug, Parser)]
pub struct DeleteOpt {
    /// path to the connector config
    #[clap(short = 'c', long)]
    config: String,
}

impl DeleteOpt {
    pub async fn execute(self) -> anyhow::Result<()> {
        let config = ConnectorConfig::from_file(self.config)?;

        let k8_config = K8Config::load().expect("no k8 config found");
        let namespace = k8_config.namespace().to_owned();

        let client = k8_client::new_shared(k8_config).expect("failed to create k8 client");
        let metadata = InputObjectMeta {
            name: config.name,
            namespace,
            ..Default::default()
        };
        client
            .delete_item::<DeploymentSpec, InputObjectMeta>(&metadata)
            .await?;
        Ok(())
    }
}
