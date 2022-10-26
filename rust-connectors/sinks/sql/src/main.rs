use std::sync::Arc;

use fluvio::dataplane::record::Record;
use fluvio_connectors_common::metrics::ConnectorMetrics;
use fluvio_connectors_common::monitoring::init_monitoring;
use fluvio_future::tracing::{debug, info};

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
    let metrics = Arc::new(ConnectorMetrics::new());
    init_monitoring(metrics.clone());
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

    let mut stream = raw_opts.common.create_consumer_stream("sql").await?;
    info!("connected to fluvio stream");

    let mut transformations = Transformations::from_chain(
        raw_opts
            .common
            .transform_common
            .create_smart_module_chain()
            .await?
            .unwrap_or_default(),
    )
    .await?;
    debug!("{:?} transformations loaded", transformations);

    info!(
        "starting stream processing from {}",
        raw_opts.common.fluvio_topic
    );
    while let Some(Ok(consumer_record)) = stream.next().await {
        let record: Record = consumer_record.into_inner();
        let output = transformations.transform(record)?;
        for output_record in output {
            let operation: Operation = serde_json::from_slice(output_record.value.as_ref())?;
            debug!("{:?}", operation);
            db.execute(operation).await?;
        }
    }

    Ok(())
}
