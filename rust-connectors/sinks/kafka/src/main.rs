use fluvio_connectors_common::opt::{CommonSourceOpt, Record};
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
            "direction": "Source",
            "schema": schema_for!(KafkaOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let opts: KafkaOpt = KafkaOpt::from_args();
    opts.common.enable_logging();
    let _ = opts.execute().await?;
    Ok(())
}

#[derive(StructOpt, Debug, JsonSchema, Clone)]
pub struct KafkaOpt {
    /// A Comma separated list of the kafka brokers to connect to
    #[structopt(long, env = "KAFKA_URL", hide_env_values = true)]
    pub kafka_url: String,

    #[structopt(long)]
    pub kafka_topic: Option<String>,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
impl KafkaOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut stream = self.common.create_consumer_stream().await?;

        /*
        * TODO:
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "")
        .set("sasl.password", "")
        */
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::config::FromClientConfig;

        let admin = AdminClient::from_config(
            ClientConfig::new()
                .set("bootstrap.servers", self.kafka_url.clone())
                .set("session.timeout.ms", "45000"),
        )?;
        let kafka_topic = self
            .kafka_topic
            .as_ref()
            .unwrap_or(&self.common.fluvio_topic);
        let new_topic = NewTopic::new(kafka_topic.as_str(), 1, TopicReplication::Fixed(1));
        let _ = admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await?;

        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", self.kafka_url.clone())
            .set("session.timeout.ms", "45000")
            .create()
            .expect("Producer creation error");

        info!("Starting stream");
        while let Some(Ok(record)) = stream.next().await {
            let _ = self.send_to_kafka(&record, producer).await?;
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
        let kafka_record = FutureRecord::to(kafka_topic.as_str())
            .payload(record.value())
            .key(record.key().unwrap_or(&[]));
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
