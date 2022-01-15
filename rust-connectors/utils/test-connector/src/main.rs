use schemars::schema_for;
use serde::Serialize;
use std::env;
use std::process::Command;
use structopt::StructOpt;

mod produce;
use produce::produce;
mod opts;
use opts::{FluvioTestOpts, TestConnectorOpts};

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
    if let Ok(fluvio_test_opts) = FluvioTestOpts::from_args_safe() {
        let mut fluvio_test_args = Vec::new();

        if let Some(runner_opts) = fluvio_test_opts.runner_opts {
            let opts: Vec<String> = runner_opts
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            fluvio_test_args.extend(opts)
        };

        fluvio_test_args.push(fluvio_test_opts.test_name);
        fluvio_test_args.push("--".to_string());

        if let Some(test_opts) = fluvio_test_opts.test_opts {
            let opts: Vec<String> = test_opts
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            fluvio_test_args.extend(opts)
        };

        // Run this command in a loop
        loop {
            if let Ok(child_proc) = Command::new(fluvio_test_opts.fluvio_test_path.clone())
                .args(fluvio_test_args.clone())
                .spawn()
            {
                // Print fluvio-test's stdout
                let _ = child_proc.wait_with_output();
            }

            if fluvio_test_opts.skip_loop {
                break;
            }
        }
    } else {
        // Otherwise,
        let arguments: Vec<String> = std::env::args().collect();
        let opts = match arguments.get(1) {
            Some(schema) if schema == "metadata" => {
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
            }
            _ => {
                println!("cmd args: {:?}", arguments);
                TestConnectorOpts::from_args()
            }
        };
        let _ = produce(opts).await?;
    }
    Ok(())
}
