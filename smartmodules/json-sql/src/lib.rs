mod mapping;
mod pointer;
mod transform;

use once_cell::sync::OnceCell;

use crate::mapping::Mapping;
use eyre::ContextCompat;
use fluvio_smartmodule::{
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInternalError},
    smartmodule, Record, RecordData, Result,
};

static MAPPING: OnceCell<Mapping> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> i32 {
    if let Some(raw_mapping) = params.get("with") {
        match serde_json::from_str(raw_mapping) {
            Ok(mapping) => {
                MAPPING
                    .set(mapping)
                    .expect("mapping is already uninitialized");
                0
            }
            Err(err) => {
                eprintln!("unable to parse init params: {:?}", err);
                SmartModuleInternalError::InitParamsParse as i32
            }
        }
    } else {
        SmartModuleInternalError::InitParamsNotFound as i32
    }
}

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let mapping = MAPPING
        .get()
        .wrap_err("using of uninitialized mapping. Forgot call constructor?")?;

    let key = record.key.clone();
    let record = serde_json::from_slice(record.value.as_ref())?;
    let transformed = transform::transform(record, mapping)?;

    Ok((key, serde_json::to_vec(&transformed)?.into()))
}
