use fluvio_connectors_common::fluvio::RecordKey;
use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::{schema_for, JsonSchema};
use structopt::StructOpt;
use tokio_stream::StreamExt;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

#[derive(StructOpt, Debug, JsonSchema)]
pub struct HttpOpt {
    /// Endpoint for the http connector
    #[structopt(long)]
    endpoint: String,

    /// HTTP body for the request
    #[structopt(long)]
    body: Option<String>,

    /// HTTP method used in the request. Eg. GET, POST, PUT...
    #[structopt(long, default_value = "GET")]
    method: String,

    /// Interval between each request
    #[structopt(long, default_value = "300")]
    interval: u64,

    /// Headers to include in the HTTP request, in "Key=Value" format
    #[structopt(long = "header", alias = "headers")]
    headers: Vec<String>,

    #[structopt(flatten)]
    #[schemars(flatten)]
    common: CommonSourceOpt,
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = schema_for!(HttpOpt);
        let metadata = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "source",
            "schema": schema,
        });
        let metadata_json = serde_json::to_string_pretty(&metadata).unwrap();
        println!("{}", metadata_json);
        return Ok(());
    }

    let opts: HttpOpt = HttpOpt::from_args();

    // Enable logging, setting default RUST_LOG if not given
    opts.common.enable_logging();
    if let Err(_) | Ok("") = std::env::var("RUST_LOG").as_deref() {
        std::env::set_var("RUST_LOG", "http=info");
    }

    tracing::info!("Initializing HTTP connector");
    tracing::info!(
        "Using interval={}s, method={}, topic={}, endpoint={}",
        opts.interval,
        opts.method,
        opts.common.fluvio_topic,
        opts.endpoint
    );

    let timer = tokio::time::interval(tokio::time::Duration::from_secs(opts.interval));
    let mut timer_stream = tokio_stream::wrappers::IntervalStream::new(timer);
    let producer = opts
        .common
        .create_producer()
        .await
        .expect("Failed to create producer");
    tracing::info!("Connected to Fluvio");

    let client = reqwest::Client::new();
    let method: reqwest::Method = opts.method.parse()?;

    while timer_stream.next().await.is_some() {
        let mut req = client.request(method.clone(), &opts.endpoint);

        let headers = opts.headers.iter().flat_map(|h| h.split_once(':'));
        for (key, value) in headers {
            req = req.header(key, value);
        }

        if let Some(ref body) = opts.body {
            req = req.body(body.clone());
        }
        let response = req.send().await?;
        let response_text = response.text().await?;

        tracing::info!("Producing: {}", response_text);
        producer.send(RecordKey::NULL, response_text).await?;
    }

    Ok(())
}
