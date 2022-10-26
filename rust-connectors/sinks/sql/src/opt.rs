use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use schemars::JsonSchema;
use url::Url;

/// The JSON SQL Connector for Fluvio.
#[derive(Parser, Debug, JsonSchema, Clone)]
#[clap(settings = &[clap::AppSettings::DeriveDisplayOrder])]
pub struct SqlConnectorOpt {
    /// The connection string for the SQL database
    ///
    /// Ex: postgres://user:password@localhost:5432/database_name
    #[clap(long, env = "FLUVIO_DATABASE_URL", hide_env_values = true)]
    pub database_url: Url,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}
