//! reqwest input helper

use crate::formatter::HttpHeader;
use crate::formatter::HttpRecordError;
use crate::formatter::HttpResponseRecord;

impl TryFrom<&reqwest::Response> for HttpResponseRecord {
    type Error = HttpRecordError;

    fn try_from(response: &reqwest::Response) -> Result<Self, Self::Error> {
        let (status_code, status_string) = status(&response.status());

        Ok(Self {
            version: Some(version(&response.version())),
            status_code,
            status_string,
            headers: Some(headers(response.headers())),
            body: None,
            output_type: None,
            output_parts: None,
        })
    }
}

// Reqwest Response TryFrom helper impl.
fn version(version: &reqwest::Version) -> String {
    format!("{:?}", version)
}

// Reqwest Response TryFrom helper impl.
fn status(status: &reqwest::StatusCode) -> (Option<u16>, Option<String>) {
    let status_code = status.as_u16();

    let status_string = status.canonical_reason().map(|s| s.to_string());

    (Some(status_code), status_string)
}

// Reqwest Response TryFrom helper impl.
fn headers(hdr_map: &reqwest::header::HeaderMap) -> Vec<HttpHeader> {
    let mut hdr_vec = Vec::with_capacity(hdr_map.len());

    for (hdr_key, hdr_val) in hdr_map.iter() {
        let hdr_value = match hdr_val.to_str() {
            Ok(v) => v.to_owned().to_string(),
            // Should we warn / error / spend effort on opportunistic lossy header?
            Err(_) => String::from(""),
        };

        hdr_vec.push(HttpHeader {
            name: hdr_key.to_owned().to_string(),
            value: hdr_value,
        });
    }

    hdr_vec
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formatter::tests::data_test_header;

    use rstest::rstest;

    #[allow(unused_imports)]
    use rstest_reuse::{self, *};

    #[rstest(fuzz_input, case("basic"))]
    fn test_valid_format_reqwest_one_header(fuzz_input: &str) {
        let (test_header_map, expected_result, _) = data_test_header(fuzz_input, fuzz_input, 1);
        assert_eq!(headers(&test_header_map), expected_result);
    }
    // Unicorns not welcome as headers :(
    #[rstest(fuzz_input, case("🦄"))]
    #[should_panic]
    fn test_invalid_format_reqwest_one_header(fuzz_input: &str) {
        let (test_header_map, expected_result, _) = data_test_header(fuzz_input, fuzz_input, 1);
        assert_eq!(headers(&test_header_map), expected_result);
    }
}
