use fluvio_connectors_common::fluvio::RecordKey;
use fluvio_connectors_common::git_hash_version;
use fluvio_connectors_common::opt::CommonConnectorOpt;

mod error;
use error::MqttConnectorError;

use fluvio_future::tracing::{debug, error, info};
use rumqttc::{v4::Packet, AsyncClient, ClientConfig, Event, MqttOptions, QoS, Transport};
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use std::convert::TryFrom;
use std::time::Duration;
use structopt::StructOpt;
use url::Url;

#[derive(StructOpt, Debug, JsonSchema)]
struct MqttOpts {
    #[structopt(long)]
    timeout: Option<u64>,

    #[structopt(short, long, env = "MQTT_URL", hide_env_values = true)]
    mqtt_url: String,

    #[structopt(long)]
    mqtt_topic: String,

    #[structopt(long)]
    client_id: Option<String>,

    #[structopt(flatten)]
    #[schemars(flatten)]
    common: CommonConnectorOpt,
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

    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting MQTT source connector",
    );

    async_global_executor::block_on(async move {
        let mqtt_timeout_seconds = Duration::from_secs(opts.timeout.unwrap_or(60));
        let mqtt_topic = opts.mqtt_topic;

        let client_id = opts
            .client_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let mut url = Url::parse(&opts.mqtt_url)?;
        if !url.query_pairs().any(|(key, _)| key == "client_id") {
            url.query_pairs_mut().append_pair("client_id", &client_id);
        }

        {
            let mut url_without_password = url.clone();
            let _ = url_without_password.set_password(None);
            info!(
                timout=&opts.timeout,
                mqtt_url=%url_without_password,
                fluvio_topic=%opts.common.fluvio_topic,
                %mqtt_topic,
                %client_id
            );
        }
        let mut mqttoptions = MqttOptions::try_from(url.clone())?;
        mqttoptions.set_keep_alive(mqtt_timeout_seconds);
        if url.scheme() == "mqtts" || url.scheme() == "ssl" {
            let mut root_cert_store = rustls::RootCertStore::empty();
            for cert in
                rustls_native_certs::load_native_certs().expect("could not load platform certs")
            {
                root_cert_store
                    .add(&rustls::Certificate(cert.0))
                    .expect("Failed to parse DER");
            }
            let client_config = ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();

            mqttoptions.set_transport(Transport::tls_with_config(client_config.into()));
        }
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
                        error!("Mqtt error {}", e);
                        break;
                    }
                };
                if let Ok(mqtt_event) = MqttEvent::try_from(notification) {
                    let fluvio_record = serde_json::to_string(&mqtt_event)?;
                    debug!("Record before smartstream {}", fluvio_record);
                    if let Err(e) = producer.send(RecordKey::NULL, fluvio_record).await {
                        error!("Fluvio error! {}", e);
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
