use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use schemars::JsonSchema;
use url::Url;

/// The Postgres CDC Connector for Fluvio.
#[derive(Parser, Debug, JsonSchema, Clone)]
#[clap(settings = &[clap::AppSettings::DeriveDisplayOrder])]
pub struct PgConnectorOpt {
    /// The URL of the postgres leader database to stream from
    ///
    /// Ex: postgres://user:password@localhost:5432/database_name
    #[clap(long, env = "FLUVIO_PG_DATABASE_URL", hide_env_values = true)]
    pub url: Url,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}
