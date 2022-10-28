use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_connectors_common::{common_initialize, git_hash_version};
use fluvio_future::tracing::{error, info, debug};
use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use std::time::Duration;
use schemars::schema_for;
use schemars::JsonSchema;
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
    let mut stream = raw_opts.common.create_consumer_stream("kafka").await?;
    let mut kafka_client = KafkaClient::new(vec![raw_opts.kafka_url.clone()]);
    let _ = kafka_client.load_metadata_all()?;
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
