pub type Result<T, E = Error> = core::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("expected message body to be a UTF-8 string")]
    InvalidString(#[source] std::io::Error),
    #[error("encountered unexpected message type in replication stream")]
    UnexpectedMessage,
    #[error("missing schema info for table with OID={0}")]
    MissingSchema(u32),
    #[error("encountered type with unknown OID: {0}")]
    UnrecognizedType(u32),
    #[error("failed to parse tuple data: {0}")]
    ParseError(String),
    #[error("failed to find specified Fluvio Topic: {0}")]
    TopicNotFound(String),
    #[error(
        "failed to open Postgres replication stream
check that your database has a PUBLICATION '{publication}' and slot '{slot}'"
    )]
    PostgresReplication {
        publication: String,
        slot: String,
        source: tokio_postgres::Error,
    },
}
