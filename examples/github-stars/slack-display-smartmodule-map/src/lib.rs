use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(filter)]
pub fn filter_log_level(record: &Record) -> Result<bool> {
    let stars = serde_json::from_slice::<GithubStars>(record.value.as_ref())?;
    Ok(stars.star_update)
}
/*

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let stars = serde_json::from_slice::<GithubStars>(record.value.as_ref())?;
    let count = format!("{}", stars.stargazers_count);
    let key = record.key.clone();

    Ok((key, count.into()))
}
#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> Result<RecordData> {
    // Parse accumulator
    let accumulated_stars: GithubStars =
        serde_json::from_slice(accumulator.as_ref()).unwrap_or_default();

    // Parse next record
    let new_stars: GithubStars = serde_json::from_slice(current.value.as_ref())?;
    todo!();

    /*
    // Add stars and serialize
    let summed_stars = accumulated_stars + new_stars;
    let summed_stars_bytes = serde_json::to_vec_pretty(&summed_stars)?;

    Ok(summed_stars_bytes.into())
    */
}
*/
#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
struct GithubStars {
    pub stargazers_count: i32,
    #[serde(default)]
    pub star_update: bool,
}
