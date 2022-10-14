use once_cell::sync::OnceCell;

use eyre::ContextCompat;
use fluvio_jolt::TransformSpec;
use fluvio_smartmodule::{
    dataplane::smartmodule::SmartModuleExtraParams, smartmodule, Record, RecordData, Result,
};

static SPEC: OnceCell<TransformSpec> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(raw_spec) = params.get("spec") {
        match serde_json::from_str(raw_spec) {
            Ok(spec) => {
                SPEC.set(spec).expect("spec is already initialized");
                Ok(())
            }
            Err(err) => {
                eprintln!("unable to parse spec from params: {:?}", err);
                Err(eyre::Report::msg(
                    "could not parse the specification from `spec` param",
                ))
            }
        }
    } else {
        Err(eyre::Report::msg("no jolt specification supplied"))
    }
}

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let mapping = SPEC.get().wrap_err("jolt spec is not initialized")?;

    let key = record.key.clone();
    let record = serde_json::from_slice(record.value.as_ref())?;
    let transformed = fluvio_jolt::transform(record, mapping);

    Ok((key, serde_json::to_vec(&transformed)?.into()))
}
