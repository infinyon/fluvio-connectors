use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio_connectors_common::opt::CommonSourceOpt;
use fluvio_dataplane_protocol::smartstream::SmartStreamInput;
use fluvio_future::tracing::debug;
use fluvio_smartengine::SmartStream;
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

    #[structopt(flatten)]
    #[schemars(flatten)]
    common: CommonSourceOpt,
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
    opts.common.enable_logging();

    let timer = tokio::time::interval(tokio::time::Duration::from_secs(opts.interval));
    let mut timer_stream = tokio_stream::wrappers::IntervalStream::new(timer);
    let fluvio = fluvio::Fluvio::connect().await?;
    let producer = fluvio.topic_producer(&opts.common.fluvio_topic).await?;

    let mut smart_stream: Option<Box<dyn SmartStream>> = match opts.common.smarstream_module() {
        Ok(maybe_smartstream) => maybe_smartstream,
        Err(_) => {
            let admin = fluvio.admin().await;

            let smartmodule_spec_list = &admin
                .list::<SmartModuleSpec, _>(vec![opts
                    .common
                    .smartmodule_name()
                    .expect("Not named smartmodule")
                    .into()])
                .await
                .expect("Failed to get smartmodule");

            let smartmodule_spec = &smartmodule_spec_list
                .first()
                .expect("Not found smartmodule")
                .spec;

            opts.common
                .smart_stream_module_from_spec(smartmodule_spec)
                .await
                .expect("Failed to create smartmodule")
        }
    };

    let client = reqwest::Client::new();
    let method: reqwest::Method = opts.method.parse()?;

    while timer_stream.next().await.is_some() {
        let mut req = client
            .request(method.clone(), &opts.endpoint)
            .header("Content-Type", "application/json");

        if let Some(ref body) = opts.body {
            req = req.body(body.clone());
        }
        let response = req.send().await?;

        let response_text = response.text().await?;

        if let Some(ref mut smart_stream) = smart_stream {
            debug!("Record before smartstream {:?}", response_text);
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
