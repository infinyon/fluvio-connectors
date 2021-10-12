use schemars::JsonSchema;
use structopt::StructOpt;

#[derive(StructOpt, Debug, JsonSchema)]
pub struct TestConnectorOpts {
    #[structopt(long)]
    pub topic: String,

    #[structopt(long)]
    pub count: Option<i64>,

    #[structopt(long)]
    pub timeout: Option<u64>,

    #[structopt(long, group("smartstream"))]
    pub smartstream_filter: Option<String>,

    #[structopt(long, group("smartstream"))]
    pub smartstream_map: Option<String>,
}
