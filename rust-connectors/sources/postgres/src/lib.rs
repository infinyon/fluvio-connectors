#[cfg(feature = "connect")]
mod connect;
pub mod error;
pub mod model;

#[cfg(feature = "connect")]
pub use self::connect::PgConnector;

pub use error::{Error, Result};
use schemars::JsonSchema;
use structopt::StructOpt;
use url::Url;

/// The Postgres CDC Connector for Fluvio.
#[derive(StructOpt, Debug, JsonSchema)]
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
    /// The name of the Fluvio topic to produce CDC events to
    #[structopt(long, env = "FLUVIO_PG_TOPIC")]
    pub topic: String,
}
