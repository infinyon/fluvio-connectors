use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_connectors_common::{common_initialize, git_hash_version};
use fluvio_future::tracing::{debug, error, info};
use humantime::parse_duration;
use kafka::client::Compression as KafkaCompression;
use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks, SecurityConfig};
use schemars::schema_for;
use schemars::JsonSchema;
use std::str::FromStr;
use std::time::Duration;
use tokio_stream::StreamExt;

#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct KafkaOpt {
    /// A Comma separated list of the kafka brokers to connect to
    #[clap(long, env = "KAFKA_URL", hide_env_values = true)]
    pub kafka_url: String,

    #[clap(long)]
    pub kafka_topic: Option<String>,

    #[clap(long)]
    pub kafka_partition: Option<i32>,

    /// A key value pair in the form key:value
    #[clap(long)]
    pub kafka_option: Vec<String>,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,

    #[clap(long)]
    pub client_id: Option<String>,

    #[clap(long)]
    pub compression: Option<Compression>,

    #[clap(long, parse(try_from_str = parse_duration))]
    pub fetch_max_wait_time: Option<Duration>,

    #[clap(long)]
    pub fetch_min_bytes: Option<i32>,

    #[clap(long)]
    pub fetch_max_bytes_per_partition: Option<i32>,

    #[clap(long)]
    pub fetch_crc_validation: Option<bool>,

    #[clap(long, parse(try_from_str = parse_duration))]
    pub retry_backoff_time: Option<Duration>,

    #[clap(long)]
    pub retry_max_attempts: Option<u32>,

    #[clap(long, parse(try_from_str = parse_duration))]
    pub connection_idle_timeout: Option<Duration>,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub security: SecurityOpt,
}
#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct SecurityOpt {
    #[clap(long)]
    pub key_file: Option<String>,

    #[clap(long)]
    pub cert_file: Option<String>,
}
use openssl::ssl::{SslConnector, SslMethod, SslFiletype, SslVerifyMode};
impl SecurityOpt {
    fn get_config(&self) -> Option<SecurityConfig> {

        // TODO use key file options.
        let (key, cert) = ("client.key".to_string(), "client.crt".to_string());

        // OpenSSL offers a variety of complex configurations. Here is an example:
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_cipher_list("DEFAULT").unwrap();
        builder
            .set_certificate_file(cert, SslFiletype::PEM)
            .unwrap();
        builder
            .set_private_key_file(key, SslFiletype::PEM)
            .unwrap();
        builder.check_private_key().unwrap();
        builder.set_default_verify_paths().unwrap();
        builder.set_verify(SslVerifyMode::PEER);
        let connector = builder.build();
        Some(SecurityConfig::new(connector))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common_initialize!();
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "schema": schema_for!(KafkaOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let raw_opts = KafkaOpt::from_args();
    raw_opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Kafka sink connector",
    );
    let mut kafka_client = if let Some(security_config) = raw_opts.security.get_config() {
        KafkaClient::new_secure(vec![raw_opts.kafka_url.clone()], security_config)
    } else  {
        KafkaClient::new(vec![raw_opts.kafka_url.clone()])
    };
    if let Some(client_id) = raw_opts.client_id {
        kafka_client.set_client_id(client_id);
    }
    if let Some(compression) = raw_opts.compression {
        kafka_client.set_compression(compression.into());
    }
    if let Some(fetch_max_wait_time) = raw_opts.fetch_max_wait_time {
        kafka_client.set_fetch_max_wait_time(fetch_max_wait_time)?;
    }
    if let Some(fetch_min_bytes) = raw_opts.fetch_min_bytes {
        kafka_client.set_fetch_min_bytes(fetch_min_bytes);
    }
    if let Some(fetch_crc_validation) = raw_opts.fetch_crc_validation {
        kafka_client.set_fetch_crc_validation(fetch_crc_validation);
    }
    if let Some(fetch_max_bytes_per_partition) = raw_opts.fetch_max_bytes_per_partition {
        kafka_client.set_fetch_max_bytes_per_partition(fetch_max_bytes_per_partition);
    }
    if let Some(retry_backoff_time) = raw_opts.retry_backoff_time {
        kafka_client.set_retry_backoff_time(retry_backoff_time);
    }
    if let Some(retry_max_attempts) = raw_opts.retry_max_attempts {
        kafka_client.set_retry_max_attempts(retry_max_attempts);
    }
    if let Some(connection_idle_timeout) = raw_opts.connection_idle_timeout {
        kafka_client.set_connection_idle_timeout(connection_idle_timeout);
    }
    kafka_client.load_metadata_all()?;

    let mut stream = raw_opts.common.create_consumer_stream("kafka").await?;
    info!("Starting stream");
    while let Some(Ok(record)) = stream.next().await {
        let kafka_topic = raw_opts
            .kafka_topic
            .as_ref()
            .unwrap_or(&raw_opts.common.fluvio_topic);
        let text = String::from_utf8_lossy(record.value());
        debug!("Sending {:?}, to kafka", text);
        let msg = ProduceMessage::new(
            kafka_topic,
            raw_opts.common.consumer_common.consumer_partition,
            record.key(),
            Some(record.value()),
        );
        let req = vec![msg];
        let resp =
            kafka_client.produce_messages(RequiredAcks::All, Duration::from_millis(10000), req);
        if let Err(e) = resp {
            error!("Error sending to kafka - {:?}", e);
        }
    }

    Ok(())
}

#[derive(Debug, Clone, JsonSchema)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
}
impl FromStr for Compression {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            _ => Ok(Self::None),
        }
    }
}
impl From<Compression> for KafkaCompression {
    fn from(c: Compression) -> KafkaCompression {
        match c {
            Compression::None => KafkaCompression::NONE,
            Compression::Gzip => KafkaCompression::GZIP,
            Compression::Snappy => KafkaCompression::SNAPPY,
        }
    }
}
