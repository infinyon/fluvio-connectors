#![allow(clippy::unnecessary_mut_passed)]
use fluvio_smartmodule::{smartmodule, Record, Result};

#[smartmodule(filter)]
pub fn filter_log_level(record: &Record) -> Result<bool> {
    let stars = serde_json::from_slice::<GithubStars>(record.value.as_ref())?;
    Ok(stars.star_update)
}

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
struct GithubStars {
    pub stargazers_count: i32,
    #[serde(default)]
    pub star_update: bool,
}
