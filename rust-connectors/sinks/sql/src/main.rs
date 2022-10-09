use fluvio::dataplane::record::Record;
use fluvio_future::tracing::{debug, info, error};

use fluvio_model_sql::Operation;
use futures::StreamExt;

use clap::Parser;
use fluvio_connectors_common::git_hash_version;
use schemars::schema_for;
use sql_sink::db::Db;
use sql_sink::opt::SqlConnectorOpt;
use sql_sink::transform::Transformations;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "Sink",
            "schema": schema_for!(SqlConnectorOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let raw_opts = SqlConnectorOpt::from_args();
    raw_opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "starting JSON SQL sink connector",
    );
    let mut db = Db::connect(raw_opts.database_url.as_str()).await?;
    info!("connected to database {}", db.kind());

    let mut stream = raw_opts.common.create_consumer_stream().await?;
    info!("connected to fluvio stream");

    let mut transformations = Transformations::from_fluvio(raw_opts.transform).await?;
    debug!("{:?} transformations loaded", transformations);

    info!(
        "starting stream processing from {}",
        raw_opts.common.fluvio_topic
    );
    while let Some(Ok(consumer_record)) = stream.next().await {
        let record: Record = consumer_record.into_inner();
        let output = match transformations.transform(record) {
            Ok(output) => output,
            Err(e) => {
                debug!("{e:?}");
                continue
            },
        };
        for output_record in output {
            let operation = match serde_json::from_slice::<Operation>(output_record.value.as_ref()) {
                Ok(operation) => operation,
                Err(e) => {
                    error!("{e:?}");
                    continue
                },
            };
            debug!("{:?}", operation);
            if let Err(e) = db.execute(operation).await {
                error!("{e:?}");
                continue
            }
        }
    }

    Ok(())
}
