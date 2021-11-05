use schemars::JsonSchema;
use structopt::StructOpt;
use fluvio_connectors_common::opt::CommonSourceOpt;

#[derive(StructOpt, Debug, JsonSchema)]
pub struct TestConnectorOpts {

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,

    #[structopt(long)]
    pub count: Option<i64>,

    #[structopt(long)]
    pub timeout: Option<u64>,

}
