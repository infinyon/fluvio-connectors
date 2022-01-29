//! Output Record Formatting

/// Assembles and allocates the final full Record string
pub fn format_full_record(
    version: &str,
    status: &str,
    header_count: usize,
    headers: &str,
    body: &str,
) -> String {
    let mut record_out_parts: Vec<String> = Vec::with_capacity(4);
    let status_line: Vec<String> = vec![version.to_owned(), status.to_owned()];
    record_out_parts.push(status_line.join(" "));
    if header_count > 0 {
        record_out_parts.push(headers.to_owned());
    }
    record_out_parts.push(String::from(""));
    record_out_parts.push(body.to_owned());
    record_out_parts.join("\n")
}

/// Format headers into full response
pub fn format_reqwest_headers(hdr_map: &reqwest::header::HeaderMap) -> String {
    let mut hdr_vec = Vec::with_capacity(hdr_map.len());

    for (hdr_key, hdr_val) in hdr_map.iter() {
        let mut hdr_kv_str: Vec<&str> = Vec::with_capacity(2);
        hdr_kv_str.push(hdr_key.as_str());

        match hdr_val.to_str() {
            Ok(v) => hdr_kv_str.push(v),
            // Should we spend effort on opportunistic lossy header?
            Err(_) => hdr_kv_str.push(""),
        };

        hdr_vec.push(hdr_kv_str.join(": "));
    }

    hdr_vec.join("\n")
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;

    use reqwest::header::{HeaderName, HeaderValue};

    use rstest::rstest;
    use rstest_reuse::{self, *};

    fn data_test_header(
        k_prefix: &str,
        v_prefix: &str,
        count_header: usize,
    ) -> (reqwest::header::HeaderMap, String) {
        let mut test_header_input = reqwest::header::HeaderMap::new();
        let mut expect_output_vec: Vec<String> = Vec::with_capacity(count_header);

        if count_header > 0 {
            for x in 1..count_header {
                let hdr_value = HeaderValue::from_str(&format!("{}-val-{}", v_prefix, x))
                    .expect("data_test_header hdr_value Error");

                let hdr_name =
                    HeaderName::from_lowercase(format!("{}-key-{}", k_prefix, x).as_bytes())
                        .expect("data_test_header hdr_name Error");

                if test_header_input.append(hdr_name, hdr_value) == false {
                    panic!("data_test_header ret.append() -> false!");
                }
                expect_output_vec.push(format!("{}-key: {}-val", k_prefix, v_prefix));
            }
        }

        (test_header_input, expect_output_vec.join("\n"))
    }

    #[rstest(fuzz_input, case("basic"), case("🦄"))]
    fn test_valid_format_reqwest_one_header(fuzz_input: &str) {
        let (test_header_map, expected_result) = data_test_header(fuzz_input, fuzz_input, 1);
        assert_eq!(format_reqwest_headers(&test_header_map), expected_result);
    }
}
