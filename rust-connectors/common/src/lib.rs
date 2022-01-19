#[cfg(feature = "fluvio-imports")]
pub mod fluvio {
    pub use fluvio::{FluvioError, PartitionConsumer, RecordKey, TopicProducer};
}

pub mod opt;
