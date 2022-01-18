use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::JsonSchema;
use structopt::StructOpt;

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

#[derive(StructOpt, Debug)]
pub struct FluvioTestOpts {
    #[structopt(long, default_value = "./fluvio-test")]
    pub fluvio_test_path: String,
    #[structopt(long)]
    pub runner_opts: Option<String>,
    #[structopt(long)]
    pub test_name: String,
    #[structopt(long)]
    pub test_opts: Option<String>,
    #[structopt(long, default_value = "false")]
    pub skip_loop: String,
}
