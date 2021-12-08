use github::{GitHubConnector, GitHubOpt};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    dotenv::dotenv()?;
    let opt: GitHubOpt = GitHubOpt::from_args();
    let connector = GitHubConnector::new(opt).await?;
    connector.run().await?;

    Ok(())
}
