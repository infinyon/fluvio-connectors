use crate::opts::TestConnectorOpts;
use fluvio_connectors_common::{
    fluvio::RecordKey, metrics::ConnectorMetrics, monitoring::init_monitoring,
};
use std::{sync::Arc, time::Duration};

pub async fn produce(opts: TestConnectorOpts) -> anyhow::Result<()> {
    let producer = opts
        .common
        .create_producer("test")
        .await
        .expect("Failed to create producer");

    let metrics = Arc::new(ConnectorMetrics::new(producer.metrics()));
    init_monitoring(metrics);

    let num_records = opts.count.unwrap_or(i64::MAX);
    let timeout = opts.timeout.unwrap_or(Duration::from_millis(1000));

    for i in 1..num_records {
        let value = format!("Hello, Fluvio! - {}", i);

        println!("Sending {}", value);

        producer.send(RecordKey::NULL, value).await?;
        async_std::task::sleep(timeout).await;
    }
    producer.flush().await?;
    Ok(())
}
