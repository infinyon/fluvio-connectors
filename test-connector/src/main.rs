use fluvio::RecordKey;

#[async_std::main]
async fn main() {
    if let Err(e) = produce().await {
        println!("Produce error: {:?}", e);
    }
}

async fn produce() -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer("test-connector").await?;

    loop {
        let value = "Hello, Fluvio!";
        producer.send(RecordKey::NULL, value).await?;
        println!("{}", value);
        async_std::task::sleep(std::time::Duration::from_secs(10)).await;
    }
}
