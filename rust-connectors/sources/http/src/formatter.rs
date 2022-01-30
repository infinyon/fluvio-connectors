//! Output Record Formatting

#[derive(thiserror::Error, Debug)]
pub enum HttpRecordError {}

#[derive(Debug, PartialEq, Eq)]
pub struct HttpResponseRecord {
    pub version: String,
    pub status_code: u16,
    pub status_string: Option<String>,
    pub headers: Option<Vec<HttpHeader>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

// Fan Out Record Impl
impl HttpResponseRecord {
    pub fn record(&self, body: Option<&str>) -> String {
        let mut record_out_parts: Vec<String> = Vec::with_capacity(4);

        // Status Line HTTP/X XXX CANONICAL
        let status_line: Vec<String> = vec![
            self.version.to_owned(),
            self.status_code.to_string(),
            self.status_string.to_owned().unwrap_or_else(|| "".to_string()),
        ];
        record_out_parts.push(status_line.join(" "));

        // Header lines foo: bar
        if let Some(headers) = &self.headers {
            let hdr_out_parts: Vec<String> = headers
                .iter()
                .map(|hdr| vec![hdr.name.to_owned(), hdr.value.to_owned()].join(": "))
                .collect();

            record_out_parts.push(hdr_out_parts.join("\n"));
        }

        // Body with an empty line between
        if let Some(body) = body {
            record_out_parts.push(String::from(""));
            record_out_parts.push(body.to_owned());
        }

        // Fan out joined full text Record
        record_out_parts.join("\n")
    }
}

// Reqwest Response TryFrom helper impl.
fn version(version: &reqwest::Version) -> String {
    format!("{:?}", version)
}

// Reqwest Response TryFrom helper impl.
fn status(status: &reqwest::StatusCode) -> (u16, Option<String>) {
    let status_code = status.as_u16();

    let status_string =
        status.canonical_reason().map(|s| s.to_string());

    (status_code, status_string)
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

// Turn reqwest Response into HttpResponseRecord
impl TryFrom<&reqwest::Response> for HttpResponseRecord {
    type Error = HttpRecordError;

    fn try_from(response: &reqwest::Response) -> Result<Self, Self::Error> {
        let (status_code, status_string) = status(&response.status());

        Ok(Self {
            version: version(&response.version()),
            status_code,
            status_string,
            headers: Some(headers(response.headers())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use reqwest::header::{HeaderName, HeaderValue};

    use rstest::rstest;

    #[allow(unused_imports)]
    use rstest_reuse::{self, *};

    fn data_test_header(
        k_prefix: &str,
        v_prefix: &str,
        count_header: usize,
    ) -> (reqwest::header::HeaderMap, Vec<HttpHeader>, String) {
        let mut test_header_input = reqwest::header::HeaderMap::new();
        let mut expect_output_vec_type: Vec<HttpHeader> = Vec::with_capacity(count_header);
        let mut expect_output_vec_str: Vec<String> = Vec::with_capacity(count_header);

        if count_header > 0 {
            for x in 0..count_header {
                let (hdr_key, hdr_val) = (
                    format!("x-{}-key-{}", k_prefix, x),
                    format!("x-{}-val-{}", v_prefix, x),
                );

                let hdr_value =
                    HeaderValue::from_str(&hdr_val).expect("data_test_header hdr_value Error");

                let hdr_name = HeaderName::from_bytes(&hdr_key.as_bytes())
                    .expect("data_test_header hdr_name Error");

                if test_header_input.append(hdr_name, hdr_value) == true {
                    panic!("data_test_header ret.append -> true! duplicate?!");
                }
                expect_output_vec_type.push(HttpHeader {
                    name: hdr_key.clone(),
                    value: hdr_val.clone(),
                });
                expect_output_vec_str.push(format!("{}: {}", hdr_key, hdr_val));
            }
        }

        (
            test_header_input,
            expect_output_vec_type,
            expect_output_vec_str.join("\n"),
        )
    }

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

    #[rstest(fuzz_body_input, case("basic"), case("🦄"))]
    fn test_valid_format_full_record(fuzz_body_input: &str) {
        let header_count = 1;

        let (_, data_headers, expected_headers) = data_test_header("basic", "basic", header_count);

        let (version, status, status_string) = ("HTTP/1.1", 200, Some("OK".to_string()));

        let expected_record = format!(
            "{} {} {}\n{}\n\n{}",
            version,
            status.to_string(),
            status_string.clone().unwrap_or("".to_string()),
            expected_headers,
            fuzz_body_input
        );

        let response_record = HttpResponseRecord {
            version: version.to_string(),
            status_code: status,
            status_string: status_string,
            headers: Some(data_headers),
        };

        let got_record = response_record.record(Some(fuzz_body_input));

        assert_eq!(got_record, expected_record);
    }
}
