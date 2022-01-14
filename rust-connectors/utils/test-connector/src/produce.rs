use crate::opts::TestConnectorOpts;
use fluvio_connectors_common::fluvio::RecordKey;

pub async fn produce(opts: TestConnectorOpts) -> anyhow::Result<()> {
    let producer = opts
        .common
        .create_producer()
        .await
        .expect("Failed to create producer");

    let num_records = opts.count.unwrap_or(i64::MAX);
    let timeout = opts.timeout.unwrap_or(1000);

    for i in 1..num_records {
        let value = format!("Hello, Fluvio! - {}", i);

        println!("Sending {}", value);

        producer.send(RecordKey::NULL, value).await?;
        async_std::task::sleep(std::time::Duration::from_millis(timeout)).await;
    }
    Ok(())
}
