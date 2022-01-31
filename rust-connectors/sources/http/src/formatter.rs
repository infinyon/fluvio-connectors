//! Output Record Formatting

// Implements TryFrom<reqwest::Response>
mod from_reqwest;

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
            self.status_string
                .to_owned()
                .unwrap_or_else(|| "".to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;

    use reqwest::header::{HeaderName, HeaderValue};

    use rstest::rstest;

    #[allow(unused_imports)]
    use rstest_reuse::{self, *};

    pub fn data_test_header(
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
