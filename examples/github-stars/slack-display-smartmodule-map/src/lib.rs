#![allow(clippy::unnecessary_mut_passed)]
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(filter_map)]
fn filter_map(record: &Record) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let stars = serde_json::from_slice::<GithubStars>(record.value.as_ref())?;

    if stars.star_update {
        let stars = format!("Fluvio Github Star count is now {}", stars.stargazers_count);
        Ok(Some((record.key.clone(), stars.into())))
    } else {
        Ok(None)
    }
}

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
struct GithubStars {
    pub stargazers_count: i32,
    #[serde(default)]
    pub star_update: bool,
}
