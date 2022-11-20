// Techdebt: Granular errors
#![allow(clippy::redundant_closure)]

use std::sync::Arc;

use fluvio_connectors_common::fluvio::RecordKey;
use fluvio_connectors_common::git_hash_version;
use fluvio_connectors_common::metrics::ConnectorMetrics;
use fluvio_connectors_common::monitoring::init_monitoring;
use tokio_stream::StreamExt;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

use ::http_source::HttpOpt;

use ::http_source::error::Error;
use fluvio_connectors_common::opt::GetOpts;

#[tokio::main]
async fn main() -> Result<()> {
    let metrics = Arc::new(ConnectorMetrics::new());
    init_monitoring(metrics.clone());

    let opts = if let Some(opts) = HttpOpt::get_opt() {
        opts
    } else {
        return Ok(());
    };

    // Enable logging, setting default RUST_LOG if not given
    opts.common.enable_logging();
    if let Err(_) | Ok("") = std::env::var("RUST_LOG").as_deref() {
        std::env::set_var("RUST_LOG", "http=info");
    }

    // Error on deprecated metadata options
    if opts.output_format.is_some() {
        panic!("ERROR: output_format has been deprecated and renamed as output_parts");
    }

    tracing::info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting HTTP source connector",
    );

    tracing::info!(
        interval = ?opts.interval,
        method = %opts.method,
        topic = %opts.common.fluvio_topic,
        output_parts = %opts.output_parts,
        output_type = %opts.output_type,
        endpoint = %opts.endpoint
    );

    let timer = tokio::time::interval(opts.interval);
    let mut timer_stream = tokio_stream::wrappers::IntervalStream::new(timer);
    let producer = opts
        .common
        .create_producer("http")
        .await
        .expect("Failed to create producer");
    tracing::info!("Connected to Fluvio");

    let client = reqwest::Client::new();
    let method: reqwest::Method = opts.method.parse()?;

    while timer_stream.next().await.is_some() {
        let mut req = client.request(method.clone(), &opts.endpoint);
        req = req.header("user-agent", opts.user_agent.clone());

        let headers = opts.headers.iter().flat_map(|h| h.split_once(':'));
        for (key, value) in headers {
            req = req.header(key, value);
        }

        if let Some(ref body) = opts.body {
            req = req.body(body.clone());
        }
        let response = req.send().await.map_err(|e| Error::Request(e))?;

        let mut formatter = ::http_source::formatter::HttpResponseRecord::try_from(&response)
            .map_err(|e| Error::Record(e))?;

        formatter
            .configure_output(&opts.output_type, &opts.output_parts)
            .expect("Unable to configure output type/parts");

        let response_body = response.text().await.map_err(|e| Error::ResponseBody(e))?;

        let record_out = formatter.record(Some(&response_body));

        tracing::debug!(%record_out, "Producing");

        let bytes_in = record_out.len() as u64;
        producer.send(RecordKey::NULL, record_out).await?;
        metrics.add_inbound_bytes(bytes_in as u64);
    }

    Ok(())
}
