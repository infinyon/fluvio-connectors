use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::JsonSchema;
use structopt::StructOpt;

#[derive(StructOpt, Debug, JsonSchema, Clone)]
pub struct HttpOpt {
    /// Endpoint for the http connector
    #[structopt(long)]
    pub endpoint: String,

    /// HTTP body for the request
    #[structopt(long)]
    pub body: Option<String>,

    /// HTTP method used in the request. Eg. GET, POST, PUT...
    #[structopt(long, default_value = "GET")]
    pub method: String,

    /// Interval between each request
    #[structopt(long, default_value = "300")]
    pub interval: u64,

    /// Headers to include in the HTTP request, in "Key=Value" format
    #[structopt(long = "header", alias = "headers")]
    pub headers: Vec<String>,

    /// DEPRECATED: Response output parts: body | full
    #[structopt(long, hidden(true))]
    pub output_format: Option<String>,

    /// Response output parts: body | full    
    #[structopt(long, default_value = "body")]
    pub output_parts: String,

    /// Response output type: text | json    
    #[structopt(long, default_value = "text")]
    pub output_type: String,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

pub mod error;
pub mod formatter;
