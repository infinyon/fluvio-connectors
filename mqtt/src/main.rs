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

    #[structopt(long)]
    fluvio_topic: String,
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
enum ConnectorDirection {
    Source,
    Sink,
    Both,
}

#[async_std::main]
async fn main() -> Result<(), MqttConnectorError> {
    fluvio_future::subscriber::init_tracer(None);
    let arguments: Vec<String> = std::env::args().collect();
    let opts = match arguments.get(1) {
        Some(schema) if schema == "metadata" => {
            let schema = schema_for!(MqttOpts);
            let mqtt_schema = MySchema {
                name: env!("CARGO_PKG_NAME"),
                version: env!("CARGO_PKG_VERSION"),
                description: env!("CARGO_PKG_DESCRIPTION"),
                direction: ConnectorDirection::Source,
                schema,
            };
            println!("{}", serde_json::to_string(&mqtt_schema).unwrap());
            return Ok(());
        }
        _ => MqttOpts::from_args(),
    };

    let mqtt_qos = opts.qos.unwrap_or(0);
    let mqtt_timeout_seconds = opts.timeout.unwrap_or(60);
    let mqtt_url = opts.mqtt_url; //"mqtt.hsl.fi";
    let mqtt_topic = opts.mqtt_topic; //"/hfp/v2/journey/#";
    let fluvio_topic = opts.fluvio_topic; //"mqtt";

    let timeout = std::time::Duration::from_secs(mqtt_timeout_seconds);
    let mut mqtt_client = MqttClient::new(CreateOptions::from(mqtt_url))?;
    mqtt_client.set_timeout(timeout);

    let rx = mqtt_client.start_consuming();

    mqtt_client.connect(None)?;
    mqtt_client.subscribe(&mqtt_topic, mqtt_qos)?;

    let fluvio_client = fluvio::Fluvio::connect().await?;
    let producer = fluvio_client.topic_producer(fluvio_topic).await?;

    for msg in rx.iter() {
        if let Some(msg) = msg {
            let mqtt_topic = msg.topic();
            let mqtt_payload = msg.payload();

            let _ = producer.send(mqtt_topic, mqtt_payload).await?;
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
