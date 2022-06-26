use fluvio_connectors_common::opt::CommonConnectorOpt;
use schemars::JsonSchema;
use structopt::StructOpt;
use url::Url;

/// The Postgres CDC Connector for Fluvio.
#[derive(StructOpt, Debug, JsonSchema, Clone)]
#[structopt(settings = &[structopt::clap::AppSettings::DeriveDisplayOrder])]
pub struct PgConnectorOpt {
    /// The URL of the postgres leader database to stream from
    ///
    /// Ex: postgres://user:password@localhost:5432/database_name
    #[structopt(long, env = "FLUVIO_PG_DATABASE_URL", hide_env_values = true)]
    pub url: Url,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}
