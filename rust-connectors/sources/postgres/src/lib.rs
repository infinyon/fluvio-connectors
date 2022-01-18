mod connect;
pub mod convert;
pub mod error;

pub use self::connect::PgConnector;
pub use error::{Error, Result};
use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::JsonSchema;
use structopt::StructOpt;
use url::Url;

/// The Postgres CDC Connector for Fluvio.
#[derive(StructOpt, Debug, JsonSchema)]
#[structopt(settings = &[structopt::clap::AppSettings::DeriveDisplayOrder])]
pub struct PgConnectorOpt {
    /// The URL of the postgres leader database to stream from
    ///
    /// Ex: postgres://user:password@localhost:5432/database_name
    #[structopt(long, env = "FLUVIO_PG_DATABASE_URL", hide_env_values = true)]
    pub url: Url,
    /// The name of the PUBLICATION in the leader database to monitor
    ///
    /// Ex: fluvio_cdc
    ///
    /// Before using this connector, you will need to create a publication
    /// in your leader database, using a SQL command such as
    /// "CREATE PUBLICATION fluvio_cdc FOR ALL TABLES;"
    #[structopt(long, env = "FLUVIO_PG_PUBLICATION")]
    pub publication: String,
    /// The name of the logical replication slot to stream changes from
    #[structopt(long, env = "FLUVIO_PG_SLOT")]
    pub slot: String,

    /// The time (in millis) to wait while fetching latest Fluvio record to resume
    #[structopt(long, env = "FLUVIO_PG_RESUME_TIMEOUT", default_value = "1000")]
    pub resume_timeout: u64,

    #[structopt(long)]
    pub skip_setup: bool,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}
