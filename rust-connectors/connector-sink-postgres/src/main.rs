use fluvio_connectors_common::{common_initialize, git_hash_version};
use postgres_sink::{PgConnector, PgConnectorOpt};

use clap::Parser;
use schemars::schema_for;
use tracing::info;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    common_initialize!();
    color_backtrace::install();
    let _ = dotenv::dotenv();
    std::env::set_var("RUST_BACKTRACE", "full");

    // Set default RUST_LOG if unset or empty
    if let Err(_) | Ok("") = std::env::var("RUST_LOG").as_deref() {
        std::env::set_var("RUST_LOG", "postgres=info");
    }

    // Initialize logging
    fluvio_future::subscriber::init_logger();

    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "Source",
            "schema": schema_for!(PgConnectorOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }

    let config: PgConnectorOpt = PgConnectorOpt::from_args();
    let mut connector = PgConnector::new(config).await?;

    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Postgres sink connector",
    );
    connector.process_stream().await?;
    Ok(())
}
