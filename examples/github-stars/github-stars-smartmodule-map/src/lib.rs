use fluvio_smartmodule::{smartmodule, Record, RecordData, Result as EyreResult};

#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> EyreResult<RecordData> {
    // Parse accumulator
    let mut accumulated_stars: GithubStars =
        serde_json::from_slice(accumulator.as_ref()).unwrap_or_default();

    // Parse next record
    let current_stars: GithubStars = serde_json::from_slice(current.value.as_ref())?;
    accumulated_stars.star_update = accumulated_stars.stargazers_count != current_stars.stargazers_count;
    accumulated_stars.stargazers_count = current_stars.stargazers_count;
    let accumulated_stars = serde_json::to_vec(&accumulated_stars)?;
    Ok(accumulated_stars.into())
}


#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
struct GithubStars {
    pub stargazers_count: i32,
    #[serde(default)]
    pub star_update: bool,
}
impl TryInto<GithubStars> for Record {
    type Error = serde_json::Error;
    fn try_into(self) -> Result<GithubStars, Self::Error> {
        Ok(serde_json::from_slice(self.value.as_ref())?)
    }
}
impl TryInto<GithubStars> for RecordData {
    type Error = serde_json::Error;
    fn try_into(self) -> Result<GithubStars, Self::Error> {
        let accumulated_stars: GithubStars =
            serde_json::from_slice(self.as_ref()).unwrap_or_default();
        Ok(accumulated_stars)
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
    #[test]
    fn check_aggregator_init() {
        let accumulated_stars: GithubStars = GithubStars {
            stargazers_count: 1,
            star_update: false,
        };
        let current_stars: GithubStars = GithubStars {
            stargazers_count: 1,
            ..Default::default()
        };
        let out = aggregate(accumulated_stars.try_into().unwrap(), &current_stars.try_into().unwrap());
        assert!(out.is_ok());
        let out = out.unwrap();
        let out : GithubStars = out.try_into().unwrap();
        assert_eq!(out.stargazers_count, 1);
        assert_eq!(out.star_update, false);
    }
    #[test]
    fn check_aggregator_increment() {
        let accumulated_stars: GithubStars = GithubStars {
            stargazers_count: 1,
            ..Default::default()
        };
        let current_stars: GithubStars = GithubStars {
            stargazers_count: 2,
            ..Default::default()
        };
        let out = aggregate(accumulated_stars.try_into().unwrap(), &current_stars.try_into().unwrap());
        assert!(out.is_ok());
        let out = out.unwrap();
        let out : GithubStars = out.try_into().unwrap();
        assert_eq!(out.stargazers_count, 2);
        assert_eq!(out.star_update, true);
    }
}
