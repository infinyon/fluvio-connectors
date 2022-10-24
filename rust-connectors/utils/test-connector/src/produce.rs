use crate::opts::TestConnectorOpts;
use fluvio_connectors_common::fluvio::RecordKey;
use std::time::Duration;

pub async fn produce(opts: TestConnectorOpts) -> anyhow::Result<()> {
    let producer = opts
        .common
        .create_producer("test")
        .await
        .expect("Failed to create producer");

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
