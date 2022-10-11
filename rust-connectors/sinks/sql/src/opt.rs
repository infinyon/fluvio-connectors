use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::str::FromStr;
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

    #[clap(long)]
    pub transform: Vec<TransformOpt>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[allow(dead_code)]
#[derive(Debug, JsonSchema, Clone, Deserialize)]
pub struct TransformOpt {
    pub(crate) uses: String,
    pub(crate) invoke: String,
    pub(crate) with: BTreeMap<String, String>,
}

impl FromStr for TransformOpt {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}
