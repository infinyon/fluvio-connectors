use structopt::StructOpt;
use serde::Serialize;
use tokio_stream::StreamExt;
use prometheus_data::parse_metrics;
use schemars::JsonSchema;
use url::Url;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

const DEFAULT_INTERVAL_MILLISECONDS: &str = "5000";

#[derive(StructOpt, Debug, JsonSchema)]
struct RootOpt {
    /// The Fluvio topic to produce the scraped Prometheus data to.
    topic: String,
    /// The Prometheus endpoint to scrape, e.g. https://your.prometheus.host/metrics
    endpoint: Url,
    /// The number of milliseconds to wait between scraping the prometheus endpoint.
    #[structopt(long, default_value = DEFAULT_INTERVAL_MILLISECONDS)]
    interval_millis: u64,
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
            direction: ConnectorDirection::Source,
            schema,
        };
        println!("{}", serde_json::to_string(&mqtt_schema).unwrap());
        return Ok(());
    }

    // Initialize logger
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    let opt: RootOpt = RootOpt::from_args();

    let mut timer = {
        let duration = std::time::Duration::from_millis(opt.interval_millis);
        let interval = tokio::time::interval(duration);
        tokio_stream::wrappers::IntervalStream::new(interval)
    };

    let client = reqwest::Client::new();
    let producer = fluvio::producer(&opt.topic).await?;

    while let Some(_) = timer.next().await {
        let mut success_count = 0;

        let metrics_text = match scrape(&client, opt.endpoint.clone()).await {
            Ok(text) => text,
            Err(e) => {
                tracing::error!("failed to scrape Prometheus: {}", e);
                continue;
            }
        };

        let metrics = parse_metrics(&metrics_text);
        for metric in metrics {
            match metric {
                Ok(metric) => {
                    let json = match serde_json::to_string(&metric) {
                        Ok(json) => json,
                        Err(e) => {
                            tracing::error!("failed to serialize metric: {}", e);
                            continue;
                        }
                    };

                    let produce_result = producer.send(fluvio::RecordKey::NULL, json).await;
                    match produce_result {
                        Ok(_) => {
                            success_count += 1;
                        }
                        Err(e) => {
                            tracing::error!("failed to produce to Fluvio: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("failed to parse metrics: {}", e);
                }
            }
        }
        tracing::info!("Successfully sent {} metrics", success_count);
    }

    Ok(())
}

async fn scrape(client: &reqwest::Client, url: Url) -> Result<String> {
    let response = client.get(url).send().await?;
    let text = response.text().await?;
    Ok(text)
}
