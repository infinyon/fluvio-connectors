use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};
use serde::Deserialize;
use serde_json::Value;

#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> i32 {
    0
}

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();

    let mqtt_event: MqttEvent = serde_json::from_slice(record.value.as_ref())?;
    let payload: Value = serde_json::from_slice(mqtt_event.payload.as_ref())?;
    let value = serde_json::to_vec(&payload)?;

    Ok((key, value.into()))
}

#[derive(Deserialize)]
struct MqttEvent {
    payload: Vec<u8>,
}
