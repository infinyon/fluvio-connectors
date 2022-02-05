// Techdebt: Granular errors
#![allow(clippy::redundant_closure)]

use fluvio_connectors_common::fluvio::RecordKey;
use tokio_stream::StreamExt;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

use ::http::HttpOpt;
use schemars::schema_for;
use structopt::StructOpt;

use ::http::error::Error;

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

    // Error on deprecated metadata options
    if opts.output_format.is_some() {
        panic!("ERROR: output_format has been deprecated and renamed as output_parts");
    }

    tracing::info!("Initializing HTTP connector");
    tracing::info!(
        interval = %opts.interval,
        method = %opts.method,
        topic = %opts.common.fluvio_topic,
        output_parts = %opts.output_parts,
        output_type = %opts.output_type,
        endpoint = %opts.endpoint
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
        let response = req.send().await.map_err(|e| Error::Request(e))?;

        let mut formatter = ::http::formatter::HttpResponseRecord::try_from(&response)
            .map_err(|e| Error::Record(e))?;

        formatter
            .configure_output(&opts.output_type, &opts.output_parts)
            .expect("Unable to configure output type/parts");

        let response_body = response.text().await.map_err(|e| Error::ResponseBody(e))?;

        let record_out = formatter.record(Some(&response_body));

        tracing::debug!(%record_out, "Producing");

        producer.send(RecordKey::NULL, record_out).await?;
    }

    Ok(())
}
