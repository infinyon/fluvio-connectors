use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_connectors_common::{common_initialize, git_hash_version};
use fluvio_future::tracing::info;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use schemars::schema_for;
use schemars::JsonSchema;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common_initialize!();
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "Source",
            "schema": schema_for!(KafkaOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let opts: KafkaOpt = KafkaOpt::from_args();
    opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Kafka source connector",
    );

    opts.execute().await?;
    Ok(())
}

#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct KafkaOpt {
    #[clap(long, env = "KAFKA_URL", hide_env_values = true)]
    pub kafka_url: String,

    #[clap(long)]
    pub kafka_group: Option<String>,

    #[clap(long)]
    pub kafka_topic: Option<String>,

    #[clap(long, default_value = "0")]
    #[schemars(skip)]
    pub kafka_partition: i32,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}

impl KafkaOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let producer = self.common.create_producer("kafka").await?;
        info!("Connected to fluvio!");
        let kafka_topic = self
            .kafka_topic
            .as_ref()
            .unwrap_or(&self.common.fluvio_topic);

        let mut consumer = Consumer::from_hosts(vec![self.kafka_url.clone()])
            .with_topic_partitions(kafka_topic.clone(), &[self.kafka_partition])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group(
                self.kafka_group
                    .clone()
                    .unwrap_or_else(|| "fluvio-kafka-source".to_string()),
            )
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()?;

        info!("Connected to kafka!");
        loop {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    info!("Sending {:?} to fluvio", m);
                    producer.send(m.key, m.value).await?;
                }
                consumer.consume_messageset(ms)?;
            }
            consumer.commit_consumed()?;
        }
    }
}
