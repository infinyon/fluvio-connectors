//! JSON output helper

use crate::formatter::HttpHeader;
use crate::formatter::HttpRecordError;
use crate::formatter::HttpResponseRecord;

use serde::Serialize;
use std::collections::HashMap;

/// JSON Record (Response Status) Serialisation
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct HttpJsonStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string: Option<String>,
}

/// JSON Record (Response) Serialisation
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct HttpJsonRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<HttpJsonStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<HashMap<String, JsonHeadersValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

/// JSON Record (Header Values) Serialisation
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum JsonHeadersValue {
    One(String),
    Many(Vec<String>),
}

impl ToString for HttpJsonRecord {
    fn to_string(&self) -> String {
        // Naive. Error needs to be handled. try_string() -> Result<R, E> ?
        serde_json::to_string(&self).unwrap_or_else(|_| "{}".to_string())
    }
}

impl TryFrom<&mut HttpResponseRecord> for HttpJsonRecord {
    type Error = HttpRecordError;

    fn try_from(resp_record: &mut HttpResponseRecord) -> Result<Self, Self::Error> {
        let json_headers = resp_record.headers.as_ref().map(|h| headers_to_json(h));

        let status_version = resp_record.version.to_owned();
        let status_code = resp_record.status_code.to_owned();
        let status_string = resp_record.status_string.to_owned();

        let status_rec = match (&status_version, &status_code, &status_string) {
            (None, None, None) => None,
            _ => Some(HttpJsonStatus {
                version: status_version,
                code: status_code,
                string: status_string,
            }),
        };

        Ok(HttpJsonRecord {
            status: status_rec,
            header: json_headers,
            body: resp_record.body.to_owned(),
        })
    }
}

use crate::formatter::to_json::JsonHeadersValue::{Many, One};
use std::collections::hash_map::Entry;

fn headers_to_json(in_hdrs: &[HttpHeader]) -> HashMap<String, JsonHeadersValue> {
    let mut out = HashMap::new();

    for hdr in in_hdrs {
        if let Entry::Occupied(mut o) = out.entry(hdr.name.to_owned()) {
            match o.get_mut() {
                One(cur_val_string) => {
                    *o.into_mut() = Many(vec![cur_val_string.to_owned(), hdr.value.to_owned()]);
                }
                Many(itms) => {
                    itms.push(hdr.value.to_owned());
                }
            };
        } else {
            out.insert(hdr.name.to_owned(), One(hdr.value.to_owned()));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    use crate::formatter::tests::data_test_header;

    #[rstest(fuzz_key, fuzz_val, case("basic", "basic"), case("basic", "🦄"))]
    fn test_one_val_header(fuzz_key: &str, fuzz_val: &str) {
        let (_, vec_httpheader_input, _) = data_test_header(fuzz_key, fuzz_val, 1);
        let expected = HashMap::from([(
            format!("x-{}-key-0", fuzz_key).to_string(),
            One(format!("x-{}-val-0", fuzz_val).to_string()),
        )]);
        let tested = headers_to_json(&vec_httpheader_input);

        assert_eq!(expected, tested);
    }
    #[rstest(fuzz_key, fuzz_val, case("basic", "basic"), case("basic", "🦄"))]
    fn test_many_vals_header(fuzz_key: &str, fuzz_val: &str) {
        let hdr_key = format!("x-{}-key-0", fuzz_key).to_string();
        let hdr_val1 = format!("x-{}-val-0", fuzz_val).to_string();
        let hdr_val2 = format!("x-{}-val-1", fuzz_val).to_string();

        let vec_header_input = vec![
            HttpHeader {
                name: hdr_key.clone(),
                value: hdr_val1.clone(),
            },
            HttpHeader {
                name: hdr_key.clone(),
                value: hdr_val2.clone(),
            },
        ];

        let expected = HashMap::from([(hdr_key, Many(vec![hdr_val1, hdr_val2]))]);

        let tested = headers_to_json(&vec_header_input);

        assert_eq!(expected, tested);
    }
}
