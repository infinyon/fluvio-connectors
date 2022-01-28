use fluvio_connectors_common::fluvio::RecordKey;
use tokio_stream::StreamExt;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

//use reqwest::header::HeaderMap;
use ::http::HttpOpt;
use schemars::schema_for;
use structopt::StructOpt;

// Prototype function, using slow format!() - evaluate using ufmt
/*
fn format_headers(hdr_map: &HeaderMap) -> String {
    let mut hdr_vec = Vec::with_capacity(hdr_map.len());

    for (hdr_key, hdr_val) in hdr_map.iter() {
        let mut hdr_kv_str: Vec<&str> = Vec::with_capacity(2);
        hdr_kv_str.push(hdr_key.as_str());

        match hdr_val.to_str() {
            Ok(v) => hdr_kv_str.push(v),
            // Should we spend effort on lossy header when it isn't valid String?
            Err(e) => hdr_kv_str.push(""),
        };

        hdr_vec.push(hdr_kv_str.join(": "));
    }

    hdr_vec.join("\n")
} */

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

    tracing::info!("Initializing HTTP connector");
    tracing::info!(
        "Using interval={}s, method={}, output_format={}, topic={}, endpoint={}",
        opts.interval,
        opts.method,
        opts.output_format,
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
        let response = req.send().await.map_err(|e| Error::Request(e))?;

        let response_version = format!("{:?}", response.version());
        let response_status = response.status().to_string();
        let response_headers_full = ::http::formatter::format_reqwest_headers(response.headers());
        let response_headers_count = response.headers().len();

        let response_body = response.text().await.map_err(|e| Error::ResponseBody(e))?;

        let record_out = match opts.output_format.as_str() {
            "full" => ::http::formatter::format_full_record(
                &response_version,
                &response_status,
                response_headers_count,
                &response_headers_full,
                &response_body,
            ),

            _ => response_body,
        };

        tracing::debug!(%record_out, "Producing");

        producer.send(RecordKey::NULL, record_out).await?;
    }

    Ok(())
}
