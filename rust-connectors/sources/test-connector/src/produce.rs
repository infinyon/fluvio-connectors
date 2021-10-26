use crate::opts::TestConnectorOpts;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio::RecordKey;

pub async fn produce(opts: TestConnectorOpts) -> Result<(), fluvio::FluvioError> {
    let mut producer = fluvio::producer(opts.topic).await?;
    let num_records = opts.count.unwrap_or(i64::MAX);
    let timeout = opts.timeout.unwrap_or(1000);

    if let Some(smart_module) = opts.smartstream_filter {
        let admin = fluvio::FluvioAdmin::connect().await?;
        let modules = admin
            .list::<SmartModuleSpec, _>(vec![smart_module.clone()])
            .await?;
        let module = modules
            .first()
            .unwrap_or_else(|| panic!("Failed to find smartmodule for {}", smart_module));
        let wasm = &module.spec.wasm;
        producer = producer.wasm_filter(wasm.payload.clone(), Default::default())
    }

    if let Some(smart_module) = opts.smartstream_map {
        let admin = fluvio::FluvioAdmin::connect().await?;
        let modules = admin
            .list::<SmartModuleSpec, _>(vec![smart_module.clone()])
            .await?;
        let module = modules
            .first()
            .unwrap_or_else(|| panic!("Failed to find smartmodule for {}", smart_module));
        let wasm = &module.spec.wasm;

        producer = producer.wasm_map(wasm.payload.clone(), Default::default())
    }

    for i in 1..num_records {
        let value = format!("Hello, Fluvio! - {}", i);
        println!("Sending {}", value);
        producer.send(RecordKey::NULL, value).await?;
        async_std::task::sleep(std::time::Duration::from_millis(timeout)).await;
    }
    Ok(())
}
