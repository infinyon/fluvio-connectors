use fluvio::dataplane::record::ConsumerRecord;
use fluvio::Fluvio;
use reqwest::{Method, Url};
use std::time::Duration;
use structopt::StructOpt;
use tokio::time::sleep;

pub type Result<T> = color_eyre::Result<T>;

#[derive(StructOpt, Debug)]
pub struct GitHubOpt {
    #[structopt(long, alias = "repos")]
    repositories: Vec<String>,
    #[structopt(long, env, hide_env_values = true)]
    github_access_token: String,

    /// The name of the Fluvio topic to produce events to
    #[structopt(long)]
    fluvio_topic: String,

    /// The number of milliseconds to wait between polling the end of a stream
    #[structopt(long, default_value = "500")]
    interval_millis: u64,

    /// Whether to read starting at the beginning of the endpoint stream
    #[structopt(long)]
    from_beginning: bool,
}

pub struct GitHubConnector {
    client: reqwest::Client,
    fluvio: Fluvio,
    config: GitHubOpt,
}

impl GitHubConnector {
    pub async fn new(config: GitHubOpt) -> Result<Self> {
        let client = reqwest::Client::new();
        let fluvio = Fluvio::connect().await?;
        Ok(Self {
            client,
            fluvio,
            config,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let producer = self
            .fluvio
            .topic_producer(&self.config.fluvio_topic)
            .await?;

        let maybe_resume: Option<String> = if self.config.from_beginning {
            None
        } else if let Some(resume_from) = self.get_last_updated_at().await? {
            eprintln!(
                "Resuming from last record in topic, timestamp={}",
                resume_from
            );
            Some(resume_from)
        } else if let Some(resume_from) = self.get_last_updated_from_endpoint().await? {
            Some(resume_from)
        } else {
            None
        };

        for i in 1.. {
            let mut req = self
                .client
                .request(
                    Method::GET,
                    Url::parse("https://api.github.com/repos/infinyon/fluvio/issues").unwrap(),
                )
                .header("User-Agent", "fluvio-connect-github")
                .header("Accept", "application/vnd.github.v3+json")
                .header(
                    "Authorization",
                    format!("token {}", self.config.github_access_token),
                )
                .query(&[
                    ("state", "all"),
                    ("direction", "asc"),
                    ("sort", "updated"),
                    ("page", &i.to_string()),
                ]);

            if let Some(resume) = maybe_resume.as_deref() {
                req = req.query(&[("since", resume)]);
            }

            let res = req.send().await?;
            let json: serde_json::Value = res.json().await?;
            let json_array = json.as_array().unwrap();
            if json_array.is_empty() {
                sleep(Duration::from_millis(self.config.interval_millis)).await;
            }

            let iter = json_array.into_iter().map(|value| {
                let key = value["number"].as_i64().unwrap().to_string();
                let value = serde_json::to_vec(value).unwrap();
                (key, value)
            });

            for (key, value) in iter {
                producer.send(key, value).await?;
            }
        }

        Ok(())
    }

    async fn get_last_updated_at(&self) -> Result<Option<String>> {
        use futures_util::StreamExt;

        let consumer = self
            .fluvio
            .partition_consumer(&self.config.fluvio_topic, 0)
            .await?;
        let mut stream = consumer.stream(fluvio::Offset::from_end(1)).await?;

        let last_record: Option<core::result::Result<ConsumerRecord, _>> = tokio::select! {
            record = stream.next() => record,
            _ = sleep(Duration::from_millis(2_000)) => return Ok(None),
        };

        let record = match last_record {
            Some(Ok(record)) => record,
            _ => return Ok(None),
        };

        let json: serde_json::Value = serde_json::from_slice(record.value())?;
        let updated_at = json["updated_at"].as_str().map(|s| s.to_string());

        Ok(updated_at)
    }

    async fn get_last_updated_from_endpoint(&self) -> Result<Option<String>> {
        let req = self
            .client
            .request(
                Method::GET,
                Url::parse("https://api.github.com/repos/infinyon/fluvio/issues").unwrap(),
            )
            .header("User-Agent", "fluvio-connect-github")
            .header("Accept", "application/vnd.github.v3+json")
            .header(
                "Authorization",
                format!("token {}", self.config.github_access_token),
            )
            .query(&[
                ("state", "all"),
                ("direction", "desc"),
                ("sort", "updated"),
                ("per_page", "1"),
            ]);

        let res = req.send().await?;
        let json: serde_json::Value = res.json().await?;

        let item = json.as_array().and_then(|v| v.first());
        let updated_at = item
            .and_then(|v| v["updated_at"].as_str())
            .map(|s| s.to_string());

        Ok(updated_at)
    }
}
