//! Output Record Formatting

// Input implementations
// ---------------------------
// HttpResponseRecord Implements
// - TryFrom<reqwest::Response>
mod from_reqwest;

// Output Implementations
// ---------------------------
// HttpJsonRecord Implements
// - TryFrom<HttpResponseRecord>
// - ToString
mod to_json;
// Techdebt: Move text to mod to_text;

#[derive(thiserror::Error, Debug)]
pub enum HttpRecordError {
    #[error("Options output_parts`{0}` and/or type`{1}` Setting Error")]
    OutputOptions(String, String),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct HttpResponseRecord {
    pub version: Option<String>,
    pub status_code: Option<u16>,
    pub status_string: Option<String>,
    pub headers: Option<Vec<HttpHeader>>,
    pub body: Option<String>,
    pub output_type: Option<HttpOutputType>,
    pub output_parts: Option<HttpOutputParts>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HttpOutputType {
    HttpRecordText,
    HttpRecordJSON,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HttpOutputParts {
    HttpRecordFull,
    HttpRecordBody,
}

// Fan Out Record Impl
impl HttpResponseRecord {
    /// Configure Record output_type and output_parts together
    pub fn configure_output(
        &mut self,
        output_type: &str,
        output_parts: &str,
    ) -> Result<(), HttpRecordError> {
        let new_output_parts = match output_parts {
            "full" => Some(HttpOutputParts::HttpRecordFull),
            "body" => Some(HttpOutputParts::HttpRecordBody),
            _ => None,
        };

        let new_output_type = match output_type {
            "json" => Some(HttpOutputType::HttpRecordJSON),
            "text" => Some(HttpOutputType::HttpRecordText),
            _ => None,
        };

        if new_output_type != None && new_output_parts != None {
            self.output_type = new_output_type;
            self.output_parts = new_output_parts;

            Ok(())
        } else {
            Err(HttpRecordError::OutputOptions(
                output_type.to_string(),
                output_parts.to_string(),
            ))
        }
    }
    /// Record Fan Out based on previously set output()
    pub fn record(&mut self, body: Option<&str>) -> String {
        match self.output_type {
            Some(HttpOutputType::HttpRecordJSON) => self.record_json(body),
            Some(HttpOutputType::HttpRecordText) => self.record_text(body),

            // Naive panics?
            _ => panic!("ERROR: record called without output_parts set via output() ?"),
        }
    }
    /// JSON Record<String> Fan Out
    pub fn record_json(&mut self, body: Option<&str>) -> String {
        self.body = body.map(|b| b.to_owned());

        let json_rec = match self.output_parts {
            Some(HttpOutputParts::HttpRecordFull) => {
                to_json::HttpJsonRecord::try_from(self).unwrap()
            }
            Some(HttpOutputParts::HttpRecordBody) => {
                let mut _rec = HttpResponseRecord {
                    body: Some(body.unwrap_or("").to_owned()),
                    ..Default::default()
                };

                to_json::HttpJsonRecord::try_from(&mut _rec).unwrap()
            }
            _ => panic!("BUG record_json() Unknown JSON Record type or not set?"),
        };

        json_rec.to_string()
    }
    /// Text Record<String> Fan Out
    pub fn record_text(&self, body: Option<&str>) -> String {
        let mut record_out_parts: Vec<String> = match self.output_parts {
            Some(HttpOutputParts::HttpRecordFull) => Vec::with_capacity(4),
            Some(HttpOutputParts::HttpRecordBody) => Vec::with_capacity(1),
            None => panic!("record_text called with no HttpOutputParts?"),
        };

        // Status Line HTTP/X XXX CANONICAL
        if self.output_parts == Some(HttpOutputParts::HttpRecordFull) {
            let status_line: Vec<String> = vec![
                self.version.to_owned().unwrap_or_else(|| "".to_string()),
                self.status_code.unwrap_or(0).to_string(),
                self.status_string
                    .to_owned()
                    .unwrap_or_else(|| "".to_string()),
            ];
            record_out_parts.push(status_line.join(" "));
        }

        // Header lines foo: bar
        if self.output_parts == Some(HttpOutputParts::HttpRecordFull) {
            if let Some(headers) = &self.headers {
                let hdr_out_parts: Vec<String> = headers
                    .iter()
                    .map(|hdr| vec![hdr.name.to_owned(), hdr.value.to_owned()].join(": "))
                    .collect();

                record_out_parts.push(hdr_out_parts.join("\n"));
            }
        }

        // Body with an empty line between
        if let Some(body) = body {
            if self.output_parts == Some(HttpOutputParts::HttpRecordFull) {
                record_out_parts.push(String::from(""));
            }
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
    fn test_valid_format_full_record_text(fuzz_body_input: &str) {
        let header_count = 1;

        let (_, data_headers, expected_headers) = data_test_header("basic", "basic", header_count);

        let (version, status, status_string) = (
            Some("HTTP/1.1".to_string()),
            Some(200),
            Some("OK".to_string()),
        );

        let expected_record = format!(
            "{} {} {}\n{}\n\n{}",
            version.clone().unwrap(),
            status.clone().unwrap(),
            status_string.clone().unwrap_or("".to_string()),
            expected_headers,
            fuzz_body_input
        );

        let response_record = HttpResponseRecord {
            version: version,
            status_code: status,
            status_string: status_string,
            headers: Some(data_headers),
            body: None,
            output_parts: Some(HttpOutputParts::HttpRecordFull),
            output_type: Some(HttpOutputType::HttpRecordText),
        };

        let got_record = response_record.record_text(Some(fuzz_body_input));

        assert_eq!(got_record, expected_record);
    }
}
