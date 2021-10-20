use schemars::schema_for;
use serde::Serialize;
use structopt::StructOpt;

mod produce;
use produce::produce;
mod opts;
use opts::TestConnectorOpts;

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
    Both,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        _ => TestConnectorOpts::from_args(),
    };
    let _ = produce(opts).await?;
    Ok(())
}
