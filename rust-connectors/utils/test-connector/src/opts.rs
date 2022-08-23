use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use humantime::parse_duration;
use schemars::JsonSchema;
use std::time::Duration;

#[derive(Parser, Debug, JsonSchema)]
pub enum TestConnectorSubCmd {
    /// Return connector metadata
    Metadata,
}

#[derive(Parser, Debug, JsonSchema)]
pub struct TestConnectorOpts {
    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,

    #[clap(long)]
    pub count: Option<i64>,

    #[clap(long, parse(try_from_str = parse_duration))]
    pub timeout: Option<Duration>,

    #[clap(flatten)]
    pub test_opts: FluvioTestOpts,
}

#[derive(Parser, Debug, JsonSchema)]
pub struct FluvioTestOpts {
    /// File path to a `fluvio-test` binary
    #[clap(long, default_value = "/usr/local/bin/fluvio-test")]
    pub fluvio_test_path: String,
    /// CLI opts for `fluvio-test`
    #[clap(long)]
    pub runner_opts: Option<String>,
    /// The name of the `fluvio-test` test name to run
    #[clap(long)]
    pub test_name: Option<String>,
    /// CLI opts for a `fluvio-test` test
    #[clap(long)]
    pub test_opts: Option<String>,
    /// Tests will restart after completion unless this flag is set to "true"
    #[clap(long, default_value = "false")]
    pub skip_loop: String,
}
