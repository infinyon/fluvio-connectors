mod error;
mod formatter;
mod opt;

use std::convert::TryFrom;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_std::channel::{self, Receiver, Sender};
use async_std::task::spawn;
use clap::Parser;
use error::MqttConnectorError;
use formatter::Formatter;
use rumqttc::EventLoop;
use rumqttc::{v4::Packet, AsyncClient, Event, MqttOptions, QoS, Transport};
use rustls::ClientConfig;
use schemars::schema_for;
use serde::Deserialize;
use serde::Serialize;

use connector_common::fluvio::{RecordKey, TopicProducer};
use connector_common::{common_initialize, git_hash_version};
use fluvio_future::tracing::{debug, error, info};
use tracing::warn;

use crate::opt::{ConnectorDirection, MqttOpts};

use url::Url;

const CHANNEL_BUFFER_SIZE: usize = 10000;
const MIN_LOG_WARN_TIME: Duration = Duration::from_secs(5 * 60);

async fn mqtt_loop(
    tx: Sender<MqttEvent>,
    rx: Receiver<MqttEvent>,
    mut eventloop: EventLoop,
    should_exit: Arc<AtomicBool>,
) -> Result<(), MqttConnectorError> {
    let mut last_warn = Instant::now();
    let mut num_dropped_messages = 0u64;
    while !should_exit.load(std::sync::atomic::Ordering::Relaxed) {
        // eventloop.poll() docs state "Don't block while iterating"
        let notification = match eventloop.poll().await {
            Ok(notification) => notification,
            Err(e) => {
                error!("Mqtt error {}", e);
                return Err(MqttConnectorError::MqttConnection(e));
            }
        };

        if let Ok(mqtt_event) = MqttEvent::try_from(notification) {
            if tx.is_full() {
                num_dropped_messages += 1;
                let elapsed = last_warn.elapsed();
                if elapsed > MIN_LOG_WARN_TIME {
                    warn!("Queue backed up. Dropped {num_dropped_messages} mqtt messages in last {elapsed:?}");
                    last_warn = Instant::now();
                    num_dropped_messages = 0;
                }

                _ = rx.try_recv()
            }
            match tx.try_send(mqtt_event) {
                Ok(_) => {}
                Err(e) => match e {
                    async_std::channel::TrySendError::Full(_) => {
                        unreachable!();
                    }
                    async_std::channel::TrySendError::Closed(_) => {
                        error!("Channel closed");
                        should_exit.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Err(MqttConnectorError::ChannelClosed);
                    }
                },
            }
        }
    }
    info!("Exit signal received, exiting");
    Ok(())
}

async fn fluvio_loop(
    rx: Receiver<MqttEvent>,
    producer: TopicProducer,
    formatter: Box<dyn Formatter + Sync + Send>,
    should_exit: Arc<AtomicBool>,
) -> Result<(), MqttConnectorError> {
    let mut last_warn = Instant::now();
    let mut num_dropped_messages = 0u64;
    while !should_exit.load(std::sync::atomic::Ordering::Relaxed) {
        let mqtt_event = match rx.recv().await {
            Ok(mqtt_event) => mqtt_event,
            Err(_) => {
                error!("Channel closed");
                should_exit.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(MqttConnectorError::ChannelClosed);
            }
        };

        match formatter.to_string(&mqtt_event) {
            Ok(fluvio_record) => {
                debug!("Record before smartstream {}", fluvio_record);
                if let Err(e) = producer.send(RecordKey::NULL, fluvio_record).await {
                    error!("Fluvio error! {}", e);
                    producer.clear_errors().await;
                    fluvio_future::timer::sleep(Duration::from_secs(5)).await;
                }
            }
            Err(_) => {
                num_dropped_messages += 1;
                let elapsed = last_warn.elapsed();
                if elapsed > MIN_LOG_WARN_TIME {
                    warn!("Failed to format message. Dropped {num_dropped_messages} failed to parse messages in last {elapsed:?}");
                    last_warn = Instant::now();
                    num_dropped_messages = 0;
                }
            }
        }
    }
    info!("Exit signal received, exiting");
    Ok(())
}

fn main() -> Result<(), MqttConnectorError> {
    common_initialize!();
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

    debug!(opts = ?opts);

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

        loop {
            let producer = opts.common.create_producer("mqtt").await?;
            info!("Connected to Fluvio");
            let formatter = formatter::from_output_type(&opts.payload_output_type);
            let (client, eventloop) = AsyncClient::new(mqttoptions.clone(), 10);
            client
                .subscribe(mqtt_topic.clone(), QoS::AtMostOnce)
                .await?;
            let (tx, rx) = channel::bounded(CHANNEL_BUFFER_SIZE);
            let should_exit = Arc::new(AtomicBool::default());
            let mqtt_jh = spawn(mqtt_loop(tx, rx.clone(), eventloop, should_exit.clone()));
            let fluvio_jh = spawn(fluvio_loop(rx, producer, formatter, should_exit));
            let mqtt_result = mqtt_jh.await;
            let fluvio_result = fluvio_jh.await;
            info!("loops exited with status mqtt: {mqtt_result:?} fluvio: {fluvio_result:?}");
            info!("reconnecting after 5 seconds");
            fluvio_future::timer::sleep(Duration::from_secs(5)).await;
        }
    })
}

#[derive(Debug, Serialize)]
struct MySchema {
    name: &'static str,
    direction: ConnectorDirection,
    schema: schemars::schema::RootSchema,
    version: &'static str,
    description: &'static str,
}

#[derive(Debug, Serialize, Deserialize)]
struct MqttEvent {
    mqtt_topic: String,
    payload: Vec<u8>,
}
impl TryFrom<Event> for MqttEvent {
    type Error = String;
    fn try_from(event: Event) -> Result<Self, Self::Error> {
        match event {
            Event::Incoming(Packet::Publish(p)) => Ok(Self {
                mqtt_topic: p.topic,
                payload: p.payload.to_vec(),
            }),
            _ => Err("We don't support this event type!".to_string()),
        }
    }
}
