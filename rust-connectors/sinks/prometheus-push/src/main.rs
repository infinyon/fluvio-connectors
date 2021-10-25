#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

use fluvio::consumer::Record;
use prometheus_data::MetricFamily;
use schemars::JsonSchema;
use serde::Serialize;
use structopt::StructOpt;
use url::Url;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

#[derive(StructOpt, Debug, JsonSchema)]
struct RootOpt {
    /// The Fluvio topic to consume metrics from
    topic: String,
    /// The PushGateway endpoint address to send metrics to
    endpoint: Url,
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

#[tokio::main]
async fn main() -> Result<()> {
    // Hidden subcommand: `metadata` prints the connector metadata and schema
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = schemars::schema_for!(RootOpt);
        let mqtt_schema = MySchema {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            description: env!("CARGO_PKG_DESCRIPTION"),
            direction: ConnectorDirection::Sink,
            schema,
        };
        println!("{}", serde_json::to_string(&mqtt_schema)?);
        return Ok(());
    }

    let opt: RootOpt = RootOpt::from_args();

    let client = reqwest::Client::new();
    let consumer = fluvio::consumer(opt.topic, 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::from_end(10)).await?;

    while let Some(Ok(record)) = tokio_stream::StreamExt::next(&mut stream).await {
        let result = push_metrics(&client, opt.endpoint.clone(), &record).await;
        if let Err(e) = result {
            tracing::error!("failed to push metrics: {}", e);
        }
    }

    Ok(())
}

async fn push_metrics(client: &reqwest::Client, url: Url, record: &Record) -> Result<()> {
    let metric: MetricFamily = serde_json::from_slice(record.value())?;
    let metric_string = format!("{}", metric);
    let response = client.post(url).body(metric_string).send().await?;

    if !response.status().is_success() {
        tracing::error!(
            "HTTP error when pushing metrics: {}",
            response.status().as_str()
        );
    }
    Ok(())
}
