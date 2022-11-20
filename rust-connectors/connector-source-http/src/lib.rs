use clap::Parser;
use fluvio_connectors_common::opt::{CommonConnectorOpt, GetOpts};
use humantime::parse_duration;
use schemars::JsonSchema;
use std::time::Duration;

#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct HttpOpt {
    /// Endpoint for the http connector
    #[clap(long)]
    pub endpoint: String,

    /// HTTP body for the request
    #[clap(long)]
    pub body: Option<String>,

    /// HTTP body for the request
    #[clap(long, default_value = "fluvio/http-source 0.1.0")]
    pub user_agent: String,

    /// HTTP method used in the request. Eg. GET, POST, PUT...
    #[clap(long, default_value = "GET")]
    pub method: String,

    /// Time to wait before sending
    /// Ex: '150ms', '20s'
    #[clap(long, parse(try_from_str = parse_duration), default_value = "10s")]
    pub interval: Duration,

    /// Headers to include in the HTTP request, in "Key=Value" format
    #[clap(long = "header", alias = "headers")]
    pub headers: Vec<String>,

    /// DEPRECATED: Response output parts: body | full
    #[clap(long, hidden(true))]
    pub output_format: Option<String>,

    /// Response output parts: body | full
    #[clap(long, default_value = "body")]
    pub output_parts: String,

    /// Response output type: text | json
    #[clap(long, default_value = "text")]
    pub output_type: String,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}

impl GetOpts for HttpOpt {
    type Opt = HttpOpt;
    fn name() -> &'static str {
        env!("CARGO_PKG_NAME")
    }
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    fn description() -> &'static str {
        env!("CARGO_PKG_DESCRIPTION")
    }
}

pub mod error;
pub mod formatter;
