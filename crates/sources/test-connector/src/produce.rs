use crate::opts::TestConnectorOpts;
use fluvio::RecordKey;
use fluvio_dataplane_protocol::smartstream::{SmartStreamExtraParams, SmartStreamInput};
use fluvio_smartstream_executor::{SmartStream, SmartStreamEngine};

pub async fn produce(opts: TestConnectorOpts) -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer(opts.topic).await?;
    let num_records = opts.count.unwrap_or(i64::MAX);
    let timeout = opts.timeout.unwrap_or(1000);

    let engine = SmartStreamEngine::default();
    let mut smart_stream: Option<Box<dyn SmartStream>> = None;
    if let Some(wasm_path) = opts.smartstream_filter {
        let smart_stream_module = engine
            .create_module_from_path(wasm_path)
            .expect("Failed to read wasm path");
        smart_stream = Some(Box::new(
            smart_stream_module
                .create_filter(&engine, SmartStreamExtraParams::default())
                .expect("Failed to create smart stream filter"),
        ));
    }

    if let Some(wasm_path) = opts.smartstream_map {
        let smart_stream_module = engine
            .create_module_from_path(wasm_path)
            .expect("Failed to read wasm path");
        smart_stream = Some(Box::new(
            smart_stream_module
                .create_map(&engine, SmartStreamExtraParams::default())
                .expect("Failed to create smart stream filter"),
        ));
    }

    for i in 1..num_records {
        let value = format!("Hello, Fluvio! - {}", i);

        println!("Sending {}", value);
        if let Some(ref mut smart_stream) = smart_stream {
            let input = SmartStreamInput::from_single_record(value.as_bytes())
                .expect("Failed to get smartstream input from value");
            let output = smart_stream
                .process(input)
                .expect("Failed to process record");
            for i in &output.successes {
                producer.send(RecordKey::NULL, i.value().clone()).await?;
            }
        } else {
            producer.send(RecordKey::NULL, value).await?;
        }
        async_std::task::sleep(std::time::Duration::from_millis(timeout)).await;
    }
    Ok(())
}
