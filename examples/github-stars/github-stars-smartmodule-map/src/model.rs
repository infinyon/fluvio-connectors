use fluvio_smartmodule::{Record, RecordData};

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct GithubStars {
    pub stargazers_count: i32,
    #[serde(default)]
    pub star_update: bool,
}
impl TryInto<GithubStars> for Record {
    type Error = serde_json::Error;
    fn try_into(self) -> Result<GithubStars, Self::Error> {
        serde_json::from_slice(self.value.as_ref())
    }
}
impl From<RecordData> for GithubStars {
    fn from(record: RecordData) -> GithubStars {
        serde_json::from_slice(record.as_ref()).unwrap_or_default()
    }
}
impl TryFrom<GithubStars> for RecordData {
    type Error = serde_json::Error;
    fn try_from(value: GithubStars) -> Result<Self, Self::Error> {
        let summed_stars_bytes = serde_json::to_vec(&value)?;
        Ok(summed_stars_bytes.into())
    }
}

impl TryFrom<GithubStars> for Record {
    type Error = serde_json::Error;
    fn try_from(value: GithubStars) -> Result<Self, Self::Error> {
        let summed_stars_bytes = serde_json::to_vec(&value)?;
        Ok(Record::new(summed_stars_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serialize_check() {
        let json = include_str!("../test-data/github-response.json");
        let stars = serde_json::from_str::<GithubStars>(json);
        assert!(stars.is_ok())
    }
}
