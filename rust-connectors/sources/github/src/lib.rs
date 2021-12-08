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

    /// Fetch elements that have been updated_at a more recent time than start_date
    #[structopt(long)]
    start_date: Option<String>,
}

pub struct GitHubConnector {
    client: reqwest::Client,
    config: GitHubOpt,
}

impl GitHubConnector {
    pub fn new(config: GitHubOpt) -> Result<Self> {
        let client = reqwest::Client::new();
        Ok(Self { client, config })
    }

    pub async fn run_stream(&self) -> Result<()> {
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
                    ("page", &i.to_string()),
                ]);

            let res = req.send().await?;

            eprintln!("Response Headers: {:#?}", res.headers());
            let json: serde_json::Value = res.json().await?;
            println!("{}", json);

            let json_array = json.as_array().unwrap();
            if json_array.is_empty() {
                eprintln!("Hit empty page");
                return Ok(());
            }

            sleep(Duration::from_millis(1_000)).await;
        }

        Ok(())
    }
}
