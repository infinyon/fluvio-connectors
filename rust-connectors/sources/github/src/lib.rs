use reqwest::{Method, Url};
use structopt::StructOpt;

pub type Result<T> = color_eyre::Result<T>;

#[derive(StructOpt, Debug)]
pub struct GitHubOpt {
    #[structopt(long, alias = "repos")]
    repositories: Vec<String>,
    #[structopt(long, env, hide_env_values = true)]
    github_access_token: String,
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

    pub async fn send_request(&self) -> Result<()> {
        let res = self
            .client
            .request(
                Method::GET,
                Url::parse("https://api.github.com/repos/infinyon/infinyon-website").unwrap(),
            )
            .header("User-Agent", "fluvio-connect-github")
            .header("Accept", "application/vnd.github.v3+json")
            .header(
                "Authorization",
                format!("token {}", self.config.github_access_token),
            )
            .send()
            .await?;

        let text = res.text().await?;
        println!("Response: {}", text);
        Ok(())
    }
}
