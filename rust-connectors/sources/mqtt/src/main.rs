use fluvio_connectors_common::fluvio::RecordKey;
use fluvio_connectors_common::opt::CommonSourceOpt;

mod error;
use error::MqttConnectorError;

use fluvio_future::tracing::{debug, error, info};
use rumqttc::{v4::Packet, Event};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use std::convert::TryFrom;
use structopt::StructOpt;

#[derive(StructOpt, Debug, JsonSchema)]
struct MqttOpts {
    #[structopt(long)]
    timeout: Option<u64>,

    #[structopt(short, long)]
    mqtt_url: String,

    #[structopt(long)]
    mqtt_topic: String,

    #[structopt(long)]
    client_id: Option<String>,

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
}
fn main() -> Result<(), MqttConnectorError> {
    let arguments: Vec<String> = std::env::args().collect();
    let opts: MqttOpts = match arguments.get(1) {
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
    // Enable logging, setting default RUST_LOG if not given
    opts.common.enable_logging();

    info!("Initializing MQTT connector");

    async_global_executor::block_on(async move {
        let mqtt_timeout_seconds = opts.timeout.unwrap_or(60);
        let mqtt_url = opts.mqtt_url;
        let mqtt_topic = opts.mqtt_topic;

        let client_id = opts
            .client_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        info!(
            %mqtt_timeout_seconds,
            %mqtt_url,
            fluvio_topic=%opts.common.fluvio_topic,
            %mqtt_topic,
            %client_id
        );

        let timeout = std::time::Duration::from_secs(mqtt_timeout_seconds);

        let mut mqttoptions = MqttOptions::new(client_id, mqtt_url, 1883);
        mqttoptions.set_keep_alive(timeout);
        let producer = opts.common.create_producer().await?;

        loop {
            let (client, mut eventloop) = AsyncClient::new(mqttoptions.clone(), 10);
            client
                .subscribe(mqtt_topic.clone(), QoS::AtMostOnce)
                .await?;
            tracing::info!("Connected to Fluvio");
            loop {
                let notification = match eventloop.poll().await {
                    Ok(notification) => notification,
                    Err(e) => {
                        error!("Mqtt error {:?}", e);
                        break;
                    }
                };
                if let Ok(mqtt_event) = MqttEvent::try_from(notification) {
                    let fluvio_record = serde_json::to_string(&mqtt_event)?;
                    debug!("Record before smartstream {}", fluvio_record);
                    if let Err(e) = producer.send(RecordKey::NULL, fluvio_record).await {
                        error!("Fluvio error! {:?}", e);
                        producer.clear_errors().await;
                        break;
                    }
                }
            }
            fluvio_future::timer::sleep(std::time::Duration::from_secs(5)).await;
            debug!("reconnecting to mqtt and fluvio");
        }
    })
}

#[derive(Debug, Serialize)]
struct MqttEvent {
    mqtt_topic: String,
    payload: Vec<u8>,
}
impl TryFrom<rumqttc::Event> for MqttEvent {
    type Error = String;
    fn try_from(event: rumqttc::Event) -> Result<Self, Self::Error> {
        match event {
            Event::Incoming(Packet::Publish(p)) => Ok(Self {
                mqtt_topic: p.topic,
                payload: p.payload.to_vec(),
            }),
            _ => Err("We don't support this event type!".to_string()),
        }
    }
}
