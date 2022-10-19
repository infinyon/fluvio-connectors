use adaptive_backoff::prelude::*;
use clap::Parser;
use eyre::eyre;
use fluvio_connectors_common::{common_initialize, git_hash_version};
use postgres_source::{PgConnector, PgConnectorOpt};
use schemars::schema_for;
use tracing::{error, info};

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

    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Postgres source connector",
    );

    if let Err(err) = run_connector(&config).await {
        error!(%err,"error running connector");
    }

    Ok(())
}

async fn run_connector(config: &PgConnectorOpt) -> eyre::Result<()> {
    let mut backoff = ExponentialBackoffBuilder::default()
        .min(std::time::Duration::from_secs(1))
        .max(std::time::Duration::from_secs(60))
        .build()
        .map_err(|err| eyre!("{}", err))?;

    loop {
        let mut connector = match PgConnector::new(config.clone()).await {
            Ok(connector) => {
                backoff.reset();
                connector
            }
            Err(e) => {
                error!(%e,"error creating postgres connection");
                wait_adapatitive_backoff(&mut backoff).await;
                continue;
            }
        };

        if let Err(err) = connector.process_stream().await {
            error!(%err, "error handling postgres stream");
        }
        wait_adapatitive_backoff(&mut backoff).await;
    }
}

async fn wait_adapatitive_backoff(backoff: &mut ExponentialBackoff) {
    let wait_time = backoff.wait();
    info!(wait_time_seconds=%wait_time.as_secs(), "Waiting before retrying");
    fluvio_future::timer::sleep(wait_time).await;
}
