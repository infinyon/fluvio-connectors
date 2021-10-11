use fluvio::RecordKey;
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use structopt::StructOpt;

#[derive(StructOpt, Debug, JsonSchema)]
struct TestConnectorOpts {
    #[structopt(long)]
    topic: String,

    #[structopt(long)]
    count: Option<i64>,

    #[structopt(long)]
    timeout: Option<u64>,
}

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
    println!("ARGUMENTS TO MQTT CONNECTOR ARE: {:?}", arguments);
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

async fn produce(opts: TestConnectorOpts) -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer(opts.topic).await?;
    let num_records = opts.count.unwrap_or(i64::MAX);
    let timeout = opts.timeout.unwrap_or(1000);
    for i in 1..num_records {
        let value = format!("Hello, Fluvio! - {}", i);
        println!("Sending {}", value);
        producer.send(RecordKey::NULL, value).await?;
        async_std::task::sleep(std::time::Duration::from_millis(timeout)).await;
    }
    Ok(())
}
