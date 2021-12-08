use fluvio::Fluvio;
use reqwest::{Method, Request, Response, Url};
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

    /// Fetch elements that have been updated_at a more recent time than start_date
    #[structopt(long)]
    start_date: Option<String>,
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

    pub async fn run_stream(&self) -> Result<()> {
        let producer = self
            .fluvio
            .topic_producer(&self.config.fluvio_topic)
            .await?;

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

            if let Some(start_date) = self.config.start_date.as_deref() {
                req = req.query(&[("since", start_date)]);
            }

            let res = req.send().await?;

            eprintln!("Response Headers: {:#?}", res.headers());
            let json: serde_json::Value = res.json().await?;
            println!("{}", json);

            let json_array = json.as_array().unwrap();
            if json_array.is_empty() {
                sleep(Duration::from_millis(self.config.interval_millis)).await;
            }

            let batch: Vec<_> = json_array
                .into_iter()
                .map(|value| {
                    let key = value["number"].as_i64().unwrap().to_string();
                    let value = serde_json::to_vec(value).unwrap();
                    (key, value)
                })
                .collect();

            producer.send_all(batch).await?;
        }

        Ok(())
    }
}
