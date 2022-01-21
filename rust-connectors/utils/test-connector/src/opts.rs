use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::JsonSchema;
use structopt::StructOpt;

#[derive(StructOpt, Debug, JsonSchema)]
pub enum TestConnectorSubCmd {
    /// Return connector metadata
    Metadata,
}

#[derive(StructOpt, Debug, JsonSchema)]
pub struct TestConnectorOpts {
    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,

    #[structopt(long)]
    pub count: Option<i64>,

    #[structopt(long)]
    pub timeout: Option<u64>,

    #[structopt(flatten)]
    pub test_opts: FluvioTestOpts,
}

#[derive(StructOpt, Debug, JsonSchema)]
pub struct FluvioTestOpts {
    /// File path to a `fluvio-test` binary
    #[structopt(long, default_value = "/usr/local/bin/fluvio-test")]
    pub fluvio_test_path: String,
    /// CLI opts for `fluvio-test`
    #[structopt(long)]
    pub runner_opts: Option<String>,
    /// The name of the `fluvio-test` test name to run
    #[structopt(long)]
    pub test_name: Option<String>,
    /// CLI opts for a `fluvio-test` test
    #[structopt(long)]
    pub test_opts: Option<String>,
    /// Tests will restart after completion unless this flag is set to "true"
    #[structopt(long, default_value = "false")]
    pub skip_loop: String,
}
