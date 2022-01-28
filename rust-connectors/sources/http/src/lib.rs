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

    /// Response output format: body | full | json                                                                                                                   
    #[structopt(long, default_value = "body")]
    pub output_format: String,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

pub mod error;
pub mod formatter;
