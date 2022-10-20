use clap::Parser;
use schemars::schema_for;
use serde::Serialize;
use std::env;
use std::process::Command;

use fluvio_connectors_common::common_initialize;

mod produce;
use produce::produce;
mod opts;
use opts::{TestConnectorOpts, TestConnectorSubCmd};

#[derive(Debug, Serialize)]
struct MySchema {
    name: &'static str,
    direction: ConnectorDirection,
    schema: schemars::schema::RootSchema,
    version: &'static str,
    description: &'static str,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)] // The other variants aren't used but are part of the spec.
enum ConnectorDirection {
    Source,
    Sink,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common_initialize!();
    // Handle the subcommand
    if let Ok(TestConnectorSubCmd::Metadata) = TestConnectorSubCmd::from_args_safe() {
        let schema = schema_for!(TestConnectorOpts);
        let mqtt_schema = MySchema {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            description: env!("CARGO_PKG_DESCRIPTION"),
            direction: ConnectorDirection::Source, // When this works as a two way connector, this needs to be updated.
            schema,
        };
        println!("{}", serde_json::to_string(&mqtt_schema).unwrap());
        return Ok(());
    };
    let args = std::env::args();
    println!("ARGS {args:?}");

    // Otherwise parse the CLI flags to determine the running mode
    let opts = TestConnectorOpts::from_args();
    println!("{opts:?}");

    // If we provide a test name then we are running `fluvio-test`
    if opts.test_opts.test_name.is_some() {
        let mut fluvio_test_args = Vec::new();

        if let Some(runner_opts) = opts.test_opts.runner_opts {
            let opts: Vec<String> = runner_opts
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            fluvio_test_args.extend(opts)
        };

        fluvio_test_args.push(opts.test_opts.test_name.unwrap_or_default());
        fluvio_test_args.push("--".to_string());

        if let Some(test_opts) = opts.test_opts.test_opts {
            let opts: Vec<String> = test_opts
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            fluvio_test_args.extend(opts)
        };

        // Run this command in a loop
        loop {
            if let Ok(child_proc) = Command::new(opts.test_opts.fluvio_test_path.clone())
                .args(fluvio_test_args.clone())
                .spawn()
            {
                // Print fluvio-test's stdout
                let _ = child_proc.wait_with_output();
            }

            if &opts.test_opts.skip_loop.to_lowercase() == "true" {
                println!("Break out of loop by request");
                break;
            }

            let test_buffer_sec = 10;
            println!("Pausing {test_buffer_sec} seconds before restarting test");
            std::thread::sleep(std::time::Duration::from_secs(test_buffer_sec));
        }
    } else {
        // Otherwise run Hello, Fluvio
        println!("cmd args: {:?}", opts);
        produce(opts).await?;
    }

    Ok(())
}
