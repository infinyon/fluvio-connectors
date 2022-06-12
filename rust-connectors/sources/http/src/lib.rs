// Techdebt: Granular errors
#![allow(clippy::redundant_closure)]
use fluvio_connectors_common::fluvio::RecordKey;
use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::JsonSchema;
use structopt::StructOpt;
use tokio_stream::StreamExt;

pub mod error;
pub mod formatter;
use error::Error;
pub type HttpConnectorResult<T, E = Box<dyn std::error::Error + Send + Sync + 'static>> =
    core::result::Result<T, E>;

#[derive(StructOpt, Debug, JsonSchema, Clone, Default)]
pub struct HttpOpt {
    /// Endpoint for the http connector
    #[structopt(long)]
    pub endpoint: String,

    /// HTTP body for the request
    #[structopt(long)]
    pub body: Option<String>,

    /// HTTP body for the request
    #[structopt(long, default_value = "fluvio/http-source 0.1.0")]
    pub user_agent: String,

    /// HTTP method used in the request. Eg. GET, POST, PUT...
    #[structopt(long, default_value = "GET")]
    pub method: String,

    /// Interval between each request
    #[structopt(long, default_value = "300")]
    pub interval: u64,

    /// Headers to include in the HTTP request, in "Key=Value" format
    #[structopt(long = "header", alias = "headers")]
    pub headers: Vec<String>,

    /// DEPRECATED: Response output parts: body | full
    #[structopt(long, hidden(true))]
    pub output_format: Option<String>,

    /// Response output parts: body | full
    #[structopt(long, default_value = "body")]
    pub output_parts: String,

    /// Response output type: text | json
    #[structopt(long, default_value = "text")]
    pub output_type: String,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

impl HttpOpt {
    pub async fn execute(&self) -> HttpConnectorResult<()> {
        // Enable logging, setting default RUST_LOG if not given
        self.common.enable_logging();
        if let Err(_) | Ok("") = std::env::var("RUST_LOG").as_deref() {
            std::env::set_var("RUST_LOG", "http=info");
        }

        // Error on deprecated metadata options
        if self.output_format.is_some() {
            panic!("ERROR: output_format has been deprecated and renamed as output_parts");
        }

        tracing::info!("Initializing HTTP connector");
        tracing::info!(
            interval = %self.interval,
            method = %self.method,
            topic = %self.common.fluvio_topic,
            output_parts = %self.output_parts,
            output_type = %self.output_type,
            endpoint = %self.endpoint
        );

        let timer = tokio::time::interval(tokio::time::Duration::from_secs(self.interval));
        let mut timer_stream = tokio_stream::wrappers::IntervalStream::new(timer);
        let producer = self
            .common
            .create_producer()
            .await
            .expect("Failed to create producer");
        tracing::info!("Connected to Fluvio");

        let client = reqwest::Client::new();
        let method: reqwest::Method = self.method.parse()?;

        while timer_stream.next().await.is_some() {
            let mut req = client.request(method.clone(), &self.endpoint);
            req = req.header("user-agent", self.user_agent.clone());

            let headers = self.headers.iter().flat_map(|h| h.split_once(':'));
            for (key, value) in headers {
                req = req.header(key, value);
            }

            if let Some(ref body) = self.body {
                req = req.body(body.clone());
            }
            let response = req.send().await.map_err(|e| Error::Request(e))?;

            let mut formatter =
                formatter::HttpResponseRecord::try_from(&response).map_err(|e| Error::Record(e))?;

            formatter
                .configure_output(&self.output_type, &self.output_parts)
                .expect("Unable to configure output type/parts");

            let response_body = response.text().await.map_err(|e| Error::ResponseBody(e))?;

            let record_out = formatter.record(Some(&response_body));

            tracing::debug!(%record_out, "Producing");

            producer.send(RecordKey::NULL, record_out).await?;
        }
        Ok(())
    }
}
