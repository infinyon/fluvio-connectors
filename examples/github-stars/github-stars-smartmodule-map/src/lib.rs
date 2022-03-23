use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let stars = serde_json::from_slice::<GitHubStars>(record.value.as_ref())?;
    let key = record.key.clone();
    let simple = serde_json::to_vec(&stars)?;
    Ok((key, simple.into()))
}

#[derive(serde::Deserialize, serde::Serialize)]
struct GitHubStars {
    stargazers_count: i32,
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serialize_check() {
        let json = include_str!("../test-data/github-response.json");
        let stars = serde_json::from_str::<GitHubStars>(json);
        assert!(stars.is_ok())
    }
}
