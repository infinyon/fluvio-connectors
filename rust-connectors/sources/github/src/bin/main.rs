use github::{GitHubConnector, GitHubOpt};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    dotenv::dotenv()?;
    if let None | Some("") = std::env::var("RUST_LOG").as_deref().ok() {
        std::env::set_var("RUST_LOG", "github=info");
    }
    fluvio_future::subscriber::init_logger();

    let opt: GitHubOpt = GitHubOpt::from_args();
    let connector = GitHubConnector::new(opt).await?;
    connector.run().await?;

    Ok(())
}
