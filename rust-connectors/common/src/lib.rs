use fluvio_future::tracing::{error, info};

pub mod fluvio {
    pub use fluvio::{
        consumer::Record, metadata::topic::TopicSpec, Fluvio, FluvioError, Offset,
        PartitionConsumer, RecordKey, TopicProducer,
    };
}

pub mod config;
pub mod monitoring;
pub mod metrics;
pub(crate) mod error;
#[cfg(any(feature = "source", feature = "sink"))]
pub mod opt;

pub fn git_hash_version() -> &'static str {
    env!("GIT_HASH")
}


#[macro_export]
macro_rules! common_initialize {
    () => {
        
    };
}


