use fluvio_model_postgres::ReplicationEvent;
use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let event: ReplicationEvent = serde_json::from_slice(record.value.as_ref())?;
    let ReplicationEvent {
        wal_start,
        wal_end,
        timestamp,
        message,
    } = event;

    let new_event = ReplicationEvent {
        wal_start,
        wal_end,
        timestamp,
        message: format!("{:?}", message),
    };
    let output = serde_json::to_string(&new_event)?;
    Ok((record.key.clone(), output.into()))
}
