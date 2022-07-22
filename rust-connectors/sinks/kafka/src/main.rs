use fluvio_connectors_common::git_hash_version;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_future::tracing::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schemars::schema_for;
use schemars::JsonSchema;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // =======================================================
    // Get raw params
    // =======================================================

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

    // =======================================================
    // Setup objects and actors based on opts
    // =======================================================

    // KafkaSinkWorker members need to be inside Arc because it is referenced inside tokio::spawn
    // That expect things moved inside to be static
    // An optimization point for this would be to own the runtime (rather than calling tokio::*)
    // So that runtime::spawn does not need producer to be in Arc
    let kafka_sink_deps = KafkaSinkDependencies::from_kafka_opt(raw_opts).await?;

    // =======================================================
    // Streaming sequence starts here
    // =======================================================

    info!("Starting stream");

    let mut stream = kafka_sink_deps
        .common_connector_opt
        .create_consumer_stream()
        .await?;

    while let Some(Ok(record)) = stream.next().await {
        // Clone arcs to be sent to async block's scopes
        let producer = kafka_sink_deps.kafka_producer.clone();
        let kafka_partition = kafka_sink_deps.kafka_partition.clone();
        let kafka_topic = kafka_sink_deps.kafka_topic.clone();

        tokio::spawn(async move {
            let mut kafka_record = FutureRecord::to(kafka_topic.as_str())
                .payload(record.value())
                .key(record.key().unwrap_or(&[]));
            if let Some(kafka_partition) = &(*kafka_partition) {
                kafka_record = kafka_record.partition(*kafka_partition);
            }
            let res = producer.send(kafka_record, Duration::from_secs(0)).await;
            if let Err(e) = res {
                error!("Kafka produce {:?}", e)
            }
        });
    }

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

pub struct KafkaSinkDependencies {
    pub kafka_topic: Arc<String>,
    pub kafka_partition: Arc<Option<i32>>,
    pub kafka_producer: Arc<FutureProducer>,
    pub common_connector_opt: CommonConnectorOpt,
}

impl KafkaSinkDependencies {
    async fn from_kafka_opt(opts: KafkaOpt) -> anyhow::Result<KafkaSinkDependencies> {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::config::FromClientConfig;
        let KafkaOpt {
            kafka_url,
            kafka_topic,
            kafka_partition,
            kafka_option,
            common: common_connector_opt,
        } = opts;

        let kafka_topic = kafka_topic.unwrap_or_else(|| common_connector_opt.fluvio_topic.clone());

        let client_config = {
            let mut kafka_options: Vec<(String, String)> = Vec::new();
            for opt in &kafka_option {
                let mut splits = opt.split(':');
                let (key, value) = (
                    splits.next().unwrap().to_string(),
                    splits.next().unwrap().to_string(),
                );
                kafka_options.push((key, value));
            }
            info!("kafka_options: {:?}", kafka_options);

            let mut client_config = ClientConfig::new();
            for (key, value) in &kafka_options {
                client_config.set(key, value);
            }
            client_config.set("bootstrap.servers", kafka_url.clone());
            client_config
        };

        // =======================================================
        // Prepare topic, ensure it exists before creating producer
        // =======================================================

        let admin = AdminClient::from_config(&client_config)?;
        admin
            .create_topics(
                &[NewTopic::new(
                    kafka_topic.as_str(),
                    1,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new(),
            )
            .await?;

        let kafka_producer = client_config.create().expect("Producer creation error");

        Ok(KafkaSinkDependencies {
            kafka_topic: Arc::new(kafka_topic),
            kafka_partition: Arc::new(kafka_partition),
            kafka_producer: Arc::new(kafka_producer),
            common_connector_opt,
        })
    }
}
