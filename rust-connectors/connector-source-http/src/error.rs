pub type Result<T, E = Error> = core::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP Record formatter error")]
    Record(#[source] crate::formatter::HttpRecordError),
    #[error("HTTP Request error")]
    Request(#[source] reqwest::Error),
    #[error("HTTP Response body error")]
    ResponseBody(#[source] reqwest::Error),
}
