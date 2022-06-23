use std::time::Duration;
use humantime::parse_duration;
use schemars::JsonSchema;
use structopt::StructOpt;
use fluvio_connectors_common::opt::CommonConnectorOpt;

#[derive(StructOpt, Debug, JsonSchema)]
pub enum TestConnectorSubCmd {
    /// Return connector metadata
    Metadata,
}

#[derive(StructOpt, Debug, JsonSchema)]
pub struct TestConnectorOpts {
    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,

    #[structopt(long)]
    pub count: Option<i64>,

    #[structopt(long, parse(try_from_str = parse_duration))]
    pub timeout: Option<Duration>,

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
