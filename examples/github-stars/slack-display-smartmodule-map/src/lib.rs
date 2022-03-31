#![allow(clippy::unnecessary_mut_passed)]
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[path = "../../github-stars-smartmodule-map/src/model.rs"]
mod model;
use model::GithubStars;

#[smartmodule(filter_map)]
fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let stars = serde_json::from_slice::<GithubStars>(record.value.as_ref())?;

    if stars.star_update {
        let stars = format!("Fluvio Github Star count is {}", stars.stargazers_count);
        Ok(Some((record.key.clone(), stars.into())))
    } else {
        Ok(None)
    }
}
