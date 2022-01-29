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
