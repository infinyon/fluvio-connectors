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
                expect_output_vec.push(format!("{}: {}", hdr_key, hdr_val));
            }
        }

        (test_header_input, expect_output_vec.join("\n"))
    }

    #[rstest(fuzz_input, case("basic"))]
    fn test_valid_format_reqwest_one_header(fuzz_input: &str) {
        let (test_header_map, expected_result) = data_test_header(fuzz_input, fuzz_input, 1);
        assert_eq!(format_reqwest_headers(&test_header_map), expected_result);
    }
    // Unicorns not welcome as headers :(
    #[rstest(fuzz_input, case("🦄"))]
    #[should_panic]
    fn test_invalid_format_reqwest_one_header(fuzz_input: &str) {
        let (test_header_map, expected_result) = data_test_header(fuzz_input, fuzz_input, 1);
        assert_eq!(format_reqwest_headers(&test_header_map), expected_result);
    }

    #[rstest(fuzz_body_input, case("basic"), case("🦄"))]
    fn test_valid_format_full_record(fuzz_body_input: &str) {
        let header_count = 1;

        let (_, expected_headers) = data_test_header("basic", "basic", header_count);

        let (version, status) = ("HTTP/1.1", "200");

        let expected_record = format!(
            "{} {}\n{}\n\n{}",
            version, status, expected_headers, fuzz_body_input
        );
        let got_record = format_full_record(
            version,
            status,
            header_count,
            &expected_headers,
            fuzz_body_input,
        );

        assert_eq!(got_record, expected_record);
    }
}
