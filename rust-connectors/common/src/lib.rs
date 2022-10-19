pub mod fluvio {
    pub use fluvio::{
        consumer::Record, metadata::topic::TopicSpec, Fluvio, FluvioError, Offset,
        PartitionConsumer, RecordKey, TopicProducer,
    };
}

pub mod config;
pub(crate) mod error;
#[cfg(any(feature = "source", feature = "sink"))]
pub mod opt;

pub fn git_hash_version() -> &'static str {
    env!("GIT_HASH")
}

pub fn init_open_telemetry(pkg_name: &str, pkg_version: &str) {
    use opentelemetry::global;
    use opentelemetry::sdk::Resource;
    use opentelemetry::KeyValue;
    use std::time::Duration;

    let resource_attributes = [
        KeyValue::new("service.name", pkg_name.to_owned()),
        KeyValue::new("service.version", pkg_version.to_owned()),
    ];
    let resource = Resource::new(resource_attributes);

    let pusher = opentelemetry_otlp::new_pipeline()
        .metrics(
            opentelemetry::sdk::metrics::selectors::simple::inexpensive(),
            opentelemetry::sdk::export::metrics::aggregation::cumulative_temporality_selector(),
            opentelemetry::runtime::AsyncStd,
        )
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_period(Duration::from_secs(1))
        .with_timeout(Duration::from_secs(10))
        .with_resource(resource)
        .build()
        .unwrap();

    global::set_meter_provider(pusher);
}

#[macro_export]
macro_rules! common_initialize {
    () => {
        fluvio_connectors_common::init_open_telemetry(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    };
}