use fluvio_connectors_common::opt::CommonSourceOpt;
use fluvio_connectors_common::RecordKey;

use paho_mqtt::client::Client as MqttClient;
use paho_mqtt::CreateOptions;
mod error;
use error::MqttConnectorError;
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use structopt::StructOpt;

#[derive(StructOpt, Debug, JsonSchema)]
struct MqttOpts {
    #[structopt(short, long)]
    qos: Option<i32>,

    #[structopt(long)]
    timeout: Option<u64>,

    #[structopt(short, long)]
    mqtt_url: String,

    #[structopt(long)]
    mqtt_topic: String,

    #[structopt(flatten)]
    #[schemars(flatten)]
    common: CommonSourceOpt,
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
async fn main() -> Result<(), MqttConnectorError> {
    let arguments: Vec<String> = std::env::args().collect();
    let opts = match arguments.get(1) {
        Some(schema) if schema == "metadata" => {
            let schema = schema_for!(MqttOpts);
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
        _ => MqttOpts::from_args(),
    };
    opts.common.enable_logging();

    let mqtt_qos = opts.qos.unwrap_or(0);
    let mqtt_timeout_seconds = opts.timeout.unwrap_or(60);
    let mqtt_url = opts.mqtt_url; //"mqtt.hsl.fi";
    let mqtt_topic = opts.mqtt_topic; //"/hfp/v2/journey/#";

    let timeout = std::time::Duration::from_secs(mqtt_timeout_seconds);
    let mut mqtt_client = MqttClient::new(CreateOptions::from(mqtt_url))?;
    mqtt_client.set_timeout(timeout);

    let rx = mqtt_client.start_consuming();

    mqtt_client.connect(None)?;
    mqtt_client.subscribe(&mqtt_topic, mqtt_qos)?;

    let producer = opts.common.create_producer().await?;

    for msg in rx.iter() {
        if let Some(msg) = msg {
            let mqtt_topic = msg.topic().to_string();
            let payload = msg.payload().to_vec();
            let mqtt_event = MqttEvent {
                mqtt_topic,
                payload,
            };
            let fluvio_record = serde_json::to_string(&mqtt_event).unwrap();
            producer.send(RecordKey::NULL, fluvio_record).await?;
        } else if mqtt_client.is_connected() || !try_reconnect(&mqtt_client) {
            break;
        }
    }

    Ok(())
}

fn try_reconnect(cli: &MqttClient) -> bool {
    println!("Connection lost. Waiting to retry connection");
    for _ in 0..12 {
        // TODO: Make this back of exponentially
        std::thread::sleep(std::time::Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}
#[derive(Debug, Serialize)]
struct MqttEvent {
    mqtt_topic: String,
    payload: Vec<u8>,
}
