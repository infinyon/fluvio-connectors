use fluvio::dataplane::record::ConsumerRecord;
use fluvio::Fluvio;
use reqwest::{Method, Url};
use std::time::Duration;
use structopt::StructOpt;
use tokio::time::sleep;

pub type Result<T> = color_eyre::Result<T>;

#[derive(StructOpt, Debug)]
pub struct GitHubOpt {
    /// A list of repositories to connect to
    #[structopt(long, alias = "repos")]
    repositories: Vec<String>,

    /// A GitHub personal access token, used for accessing private repositories
    /// and allowing a greater rate-limit.
    ///
    /// This will check the `GITHUB_ACCESS_TOKEN` environment variable so keys
    /// are not required to be passed on the command-line
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

/// Connector to continuously gather data from the GitHub API
pub struct GitHubConnector {
    client: reqwest::Client,
    fluvio: Fluvio,
    config: GitHubOpt,
}

impl GitHubConnector {
    /// Create a new [`GitHubConnector`] according to the given configuration.
    pub async fn new(config: GitHubOpt) -> Result<Self> {
        let client = reqwest::Client::new();
        let fluvio = Fluvio::connect().await?;
        tracing::info!("Connected to Fluvio");
        Ok(Self {
            client,
            fluvio,
            config,
        })
    }

    /// Run the [`GitHubConnector`] in a loop, retrieving data from the GitHub API.
    pub async fn run(&self) -> Result<()> {
        let producer = self
            .fluvio
            .topic_producer(&self.config.fluvio_topic)
            .await?;

        //
        // When the connector starts, it will run some checks to determine it's behavior:
        //
        // 1) If it finds data already in the topic it's producing to, it will get the
        //    `updated_at` field from the last record and use it in the `since` query parameter
        //    in order to resume from where it left off. See `self.get_last_updated_at()`.
        // 2) If records are not found in the topic, check the `from_beginning` flag to
        //    see whether the connector should begin fetching data from the beginning of
        //    the API, and if not, from the end.
        //
        // If `maybe_resume` is set to `Some(timestamp)`, the connector will retrieve
        // elements from the API using the `?since=timestamp` query parameter. If it is
        // `None`, it will omit the `since` parameter.
        //
        let maybe_resume = if let Some(resume_from) = self.get_last_updated_at().await? {
            tracing::info!(timestamp = %resume_from, "Resuming from latest record in pre-existing topic:");
            Some(resume_from)
        } else if self.config.from_beginning {
            tracing::info!("Cannot resume from topic, streaming '--from-beginning'");
            None
        } else if let Some(resume_from) = self.get_last_updated_from_endpoint().await? {
            tracing::info!(timestamp = %resume_from, "Resuming from latest object in endpoint:");
            Some(resume_from)
        } else {
            tracing::info!("Unable to read latest endpoint object, streaming from beginning");
            None
        };

        for page in 1.. {
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
                    ("page", &page.to_string()),
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

    /// Attempt to read a record from the end of the Topic, and if one is found,
    /// use the `updated_at` field to determine where to resume reading the API from.
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

    /// Fetch the latest object from the endpoint (i.e. sort=descending) and get the
    /// `updated_at` timestamp of that object. Use that timestamp as the `since` parameter
    /// to "resume" fetching API elements from that point on.
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
