use fluvio_dataplane_protocol::smartstream::{SmartStreamExtraParams, SmartStreamInput};
use fluvio_smartengine::{SmartEngine, SmartStream};
use schemars::{schema_for, JsonSchema};
use structopt::StructOpt;
use tokio_stream::StreamExt;

type Result<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> = core::result::Result<T, E>;

#[derive(StructOpt, Debug, JsonSchema)]
pub struct HttpOpt {
    /// Endpoint for the http connector
    #[structopt(long)]
    endpoint: String,
    /// Topic to produce in the http connector
    #[structopt(long)]
    topic: String,
    /// HTTP body for the request
    #[structopt(long)]
    body: Option<String>,

    /// Method used in the request
    #[structopt(long, default_value = "GET")]
    method: String,

    /// Interval between each request
    #[structopt(long, default_value = "300")]
    interval: u64,

    #[structopt(long, group("smartstream"))]
    pub smartstream_filter: Option<String>,

    #[structopt(long, group("smartstream"))]
    pub smartstream_map: Option<String>,

    #[structopt(long, group("smartstream"))]
    pub smartstream_arraymap: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = schema_for!(HttpOpt);
        let schema_json = serde_json::to_string_pretty(&schema).unwrap();
        println!("{}", schema_json);
        return Ok(());
    }

    let opts: HttpOpt = HttpOpt::from_args();
    let timer = tokio::time::interval(tokio::time::Duration::from_secs(opts.interval));
    let mut timer_stream = tokio_stream::wrappers::IntervalStream::new(timer);
    let producer = fluvio::producer(&opts.topic).await?;

    let engine = SmartEngine::default();

    let mut smart_stream: Option<Box<dyn SmartStream>> = match (
        opts.smartstream_filter,
        opts.smartstream_map,
        opts.smartstream_arraymap,
    ) {
        (Some(filter_path), _, _) => {
            let smart_stream_module = engine
                .create_module_from_path(filter_path)
                .expect("Failed to read wasm path");
            Some(Box::new(
                smart_stream_module
                    .create_filter(&engine, SmartStreamExtraParams::default())
                    .expect("Failed to create smart stream filter"),
            ))
        }
        (_, Some(map_path), _) => {
            let smart_stream_module = engine
                .create_module_from_path(map_path)
                .expect("Failed to read wasm path");
            Some(Box::new(
                smart_stream_module
                    .create_map(&engine, SmartStreamExtraParams::default())
                    .expect("Failed to create smart stream map"),
            ))
        }
        (_, _, Some(array_map_path)) => {
            let smart_stream_module = engine
                .create_module_from_path(array_map_path)
                .expect("Failed to read wasm path");
            Some(Box::new(
                smart_stream_module
                    .create_array_map(&engine, SmartStreamExtraParams::default())
                    .expect("Failed to create smart stream array map"),
            ))
        }
        _ => None,
    };

    let client = reqwest::Client::new();
    let method: reqwest::Method = opts.method.parse()?;

    let body = opts.body.unwrap();

    while timer_stream.next().await.is_some() {
        let response = client
            .request(method.clone(), &opts.endpoint)
            .header("Content-Type", "application/json")
            .body(body.clone())
            .send()
            .await?;

        let response_text = response.text().await?;

        if let Some(ref mut smart_stream) = smart_stream {
            let input = SmartStreamInput::from_single_record(response_text.as_bytes())?;
            let output = smart_stream.process(input)?;

            let batches = output.successes.chunks(100).map(|record| {
                record
                    .iter()
                    .map(|record| (fluvio::RecordKey::NULL, record.value.as_ref()))
            });
            for batch in batches {
                producer.send_all(batch).await?;
            }
        } else {
            producer
                .send(fluvio::RecordKey::NULL, response_text)
                .await?;
        }
    }

    Ok(())
}
