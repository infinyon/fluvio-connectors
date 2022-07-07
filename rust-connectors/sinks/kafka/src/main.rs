use fluvio_connectors_common::git_hash_version;
use fluvio_connectors_common::opt::{CommonConnectorOpt, Record};
use fluvio_future::tracing::{debug, info};
use schemars::schema_for;
use schemars::JsonSchema;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let opts: KafkaOpt = KafkaOpt::from_args();
    opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Kafka sink connector",
    );

    opts.execute().await?;
    Ok(())
}

#[derive(StructOpt, Debug, JsonSchema, Clone)]
pub struct KafkaOpt {
    /// A Comma separated list of the kafka brokers to connect to
    #[structopt(long, env = "KAFKA_URL", hide_env_values = true)]
    pub kafka_url: String,

    #[structopt(long)]
    pub kafka_topic: Option<String>,

    #[structopt(long)]
    pub kafka_partition: Option<i32>,

    /// A key value pair in the form key:value
    #[structopt(long)]
    pub kafka_option: Vec<String>,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
impl KafkaOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::config::FromClientConfig;
        let kafka_topic = self
            .kafka_topic
            .as_ref()
            .unwrap_or(&self.common.fluvio_topic);

        let mut kafka_options: Vec<(String, String)> = Vec::new();
        for opt in &self.kafka_option {
            let mut splits = opt.split(':');
            let (key, value) = (
                splits.next().unwrap().to_string(),
                splits.next().unwrap().to_string(),
            );
            kafka_options.push((key, value));
        }
        info!("kafka_options: {:?}", kafka_options);

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", self.kafka_url.clone());
        for (key, value) in &kafka_options {
            client_config.set(key, value);
        }

        let admin = AdminClient::from_config(&client_config)?;

        let new_topic = NewTopic::new(kafka_topic.as_str(), 1, TopicReplication::Fixed(1));
        admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await?;

        let producer: &FutureProducer = &client_config.create().expect("Producer creation error");

        info!("Starting stream");
        let mut stream = self.common.create_consumer_stream().await?;
        while let Some(Ok(record)) = stream.next().await {
            self.send_to_kafka(&record, producer).await?;
        }
        Ok(())
    }
    pub async fn send_to_kafka(
        &self,
        record: &Record,
        kafka_producer: &FutureProducer,
    ) -> anyhow::Result<()> {
        let kafka_topic = self
            .kafka_topic
            .as_ref()
            .unwrap_or(&self.common.fluvio_topic);
        let mut kafka_record = FutureRecord::to(kafka_topic.as_str())
            .payload(record.value())
            .key(record.key().unwrap_or(&[]));
        if let Some(kafka_partition) = self.kafka_partition {
            kafka_record = kafka_record.partition(kafka_partition);
        }
        let res = kafka_producer
            .send(kafka_record, Duration::from_secs(0))
            .await;
        debug!("Kafka produce {:?}", res);
        Ok(())
    }
}
/*
use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use std::time::Duration;

impl KafkaOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut stream = self.common.create_consumer_stream().await?;
        let mut kafka_client = KafkaClient::new(vec![self.kafka_url.clone()]);
        //let mut kafka_client = KafkaClient::new(vec![self.kafka_url.clone()]);
        let _ = kafka_client.load_metadata_all()?;

        info!("Starting stream");
        while let Some(Ok(record)) = stream.next().await {
            let _ = self.send_to_kafka(&record, &mut kafka_client).await?;
        }
        Ok(())
    }
    pub async fn send_to_kafka(
        &self,
        record: &Record,
        kafka_client: &mut KafkaClient,
    ) -> anyhow::Result<()> {
        let kafka_topic = self
            .kafka_topic
            .as_ref()
            .unwrap_or(&self.common.fluvio_topic);
        let text = String::from_utf8_lossy(record.value());
        debug!("Sending {:?}, to kafka", text);
        let msg = ProduceMessage::new(
            kafka_topic,
            self.common.fluvio_partition,
            record.key(),
            Some(record.value()),
        );
        let req = vec![msg];
        let resp =
            kafka_client.produce_messages(RequiredAcks::All, Duration::from_millis(10000), req)?;
        debug!("Sent {:?}", resp);
        Ok(())
    }
}
*/
