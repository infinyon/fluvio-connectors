pub mod fluvio {
    pub use fluvio::{
        consumer::Record, metadata::topic::TopicSpec, Fluvio, FluvioError, Offset,
        PartitionConsumer, RecordKey, TopicProducer,
    };
}

pub mod config;
pub(crate) mod error;
pub mod opt;

pub fn git_hash_version() -> &'static str {
    env!("GIT_HASH")
}
