#![allow(clippy::unnecessary_mut_passed)]
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result as EyreResult};
mod model;
use model::GithubStars;

#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> EyreResult<RecordData> {
    //println!("WHERE DOES THIS GO?");
    // Parse accumulator
    //let mut accumulated_stars: GithubStars = accumulator.into();
    let mut accumulated_stars: GithubStars =
        serde_json::from_slice(accumulator.as_ref()).unwrap_or_default();
    // Parse next record
    //let current_stars: GithubStars =  current.clone().try_into()?;
    let current_stars: GithubStars = serde_json::from_slice(current.value.as_ref())?;
    accumulated_stars.star_update =
        accumulated_stars.stargazers_count != current_stars.stargazers_count;
    accumulated_stars.stargazers_count = current_stars.stargazers_count;

    let accumulated_stars: RecordData = accumulated_stars.try_into()?;
    Ok(accumulated_stars)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deserialize_check() {
        let res: Result<GithubStars, serde_json::Error> =
            serde_json::from_str("{\"stargazers_count\":950,\"star_update\":false}");
        assert!(res.is_ok());
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
        let out = aggregate(
            accumulated_stars.try_into().unwrap(),
            &current_stars.try_into().unwrap(),
        );
        assert!(out.is_ok());
        let out = out.unwrap();
        let out: GithubStars = out.try_into().unwrap();
        assert_eq!(out.stargazers_count, 1);
        assert!(!out.star_update);
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
        let out = aggregate(
            accumulated_stars.try_into().unwrap(),
            &current_stars.try_into().unwrap(),
        );
        assert!(out.is_ok());
        let out = out.unwrap();
        let out: GithubStars = out.try_into().unwrap();
        assert_eq!(out.stargazers_count, 2);
        assert!(out.star_update);
    }
}
