use anyhow::Context;
use humantime::parse_duration;
use schemars::JsonSchema;
use std::time::Duration;
use structopt::clap::AppSettings;
use structopt::StructOpt;

pub use fluvio::{
    consumer,
    consumer::{Record, SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind},
    dataplane::ErrorCode,
    metadata::topic::TopicSpec,
    Compression, ConsumerConfig, Fluvio, FluvioError, PartitionConsumer, TopicProducer,
    TopicProducerConfigBuilder,
};

use fluvio::metadata::smartmodule::SmartModuleSpec;
use tokio_stream::Stream;

#[derive(StructOpt, Debug, JsonSchema, Clone, Default)]
#[structopt(settings = &[AppSettings::DeriveDisplayOrder])]
pub struct CommonSourceOpt {
    /// The topic where this connector sends or receives records
    #[structopt(long)]
    #[schemars(skip)]
    pub fluvio_topic: String,

    #[structopt(long, default_value = "0")]
    #[schemars(skip)]
    pub fluvio_partition: i32,

    /// The rust log level. If it is not defined, `RUST_LOG` environment variable
    /// will be used. If environment variable is not defined,
    /// then INFO level will be used.
    #[structopt(long)]
    #[schemars(skip)]
    pub rust_log: Option<String>,

    /// Path of filter smartmodule used as a pre-produce step
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule"))]
    pub filter: Option<String>,

    /// Path of filter_map smartmodule used as a pre-produce step
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule"))]
    pub filter_map: Option<String>,

    /// Path of map smartmodule used as a pre-produce step
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule"))]
    pub map: Option<String>,

    /// Path of arraymap smartmodule used as a pre-produce step
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule"))]
    pub arraymap: Option<String>,

    /// Path of aggregate smartmodule used as a pre-produce step
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule"))]
    pub aggregate: Option<String>,

    #[structopt(long)]
    pub aggregate_initial_value: Option<String>,

    /// Time to wait before sending
    /// Ex: '150ms', '20s'
    #[structopt(long, parse(try_from_str = parse_duration))]
    pub linger: Option<Duration>,

    /// Compression algorithm to use when sending records.
    /// Supported values: none, gzip, snappy and lz4.
    #[structopt(long)]
    #[schemars(skip)]
    pub compression: Option<Compression>,

    /// Max amount of bytes accumulated before sending
    #[structopt(long)]
    pub batch_size: Option<usize>,
}

impl CommonSourceOpt {
    pub fn enable_logging(&self) {
        if let Some(ref rust_log) = self.rust_log {
            std::env::set_var("RUST_LOG", rust_log);
        }
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info")
        }
        fluvio_future::subscriber::init_logger();
    }
    pub async fn ensure_topic_exists(&self) -> anyhow::Result<()> {
        let admin = fluvio::FluvioAdmin::connect().await?;
        let topics = admin.list::<TopicSpec, _>(vec![]).await?;
        let topic_exists = topics.iter().any(|t| t.name == self.fluvio_topic);
        if !topic_exists {
            let _ = admin
                .create(
                    self.fluvio_topic.clone(),
                    false,
                    TopicSpec::new_computed(1, 1, Some(false)),
                )
                .await;
        }
        Ok(())
    }

    pub async fn create_producer(&self) -> anyhow::Result<TopicProducer> {
        let fluvio = fluvio::Fluvio::connect().await?;
        self.ensure_topic_exists().await?;
        let config_builder = TopicProducerConfigBuilder::default();

        // Linger
        let config_builder = if let Some(linger) = self.linger {
            config_builder.linger(linger)
        } else {
            config_builder
        };

        // Compression
        let config_builder = if let Some(compression) = self.compression {
            config_builder.compression(compression)
        } else {
            config_builder
        };

        // Batch size
        let config_builder = if let Some(batch_size) = self.batch_size {
            config_builder.batch_size(batch_size)
        } else {
            config_builder
        };

        let config = config_builder.build()?;
        let producer = fluvio
            .topic_producer_with_config(&self.fluvio_topic, config)
            .await?;

        let producer = match (
            &self.filter,
            &self.filter_map,
            &self.map,
            &self.arraymap,
            &self.aggregate,
        ) {
            (Some(filter_path), _, _, _, _) => {
                let data = self.get_smartmodule(filter_path, &fluvio).await?;
                producer.with_filter(data, Default::default())?
            }
            (_, Some(filter_map_path), _, _, _) => {
                let data = self.get_smartmodule(filter_map_path, &fluvio).await?;
                producer.with_filter(data, Default::default())?
            }
            (_, _, Some(map_path), _, _) => {
                let data = self.get_smartmodule(map_path, &fluvio).await?;
                producer.with_map(data, Default::default())?
            }
            (_, _, _, Some(array_map_path), _) => {
                let data = self.get_smartmodule(array_map_path, &fluvio).await?;
                producer.with_array_map(data, Default::default())?
            }
            (_, _, _, _, Some(aggregate)) => {
                let data = self.get_smartmodule(aggregate, &fluvio).await?;
                let initial = self.get_aggregate_initial_value(&fluvio).await?;
                self.get_aggregate_initial_value(&fluvio).await?;
                producer.with_aggregate(data, Default::default(), initial)?
            }
            _ => producer,
        };

        Ok(producer)
    }

    async fn get_aggregate_initial_value(&self, fluvio: &Fluvio) -> anyhow::Result<Vec<u8>> {
        use tokio_stream::StreamExt;
        if let Some(initial_value) = &self.aggregate_initial_value {
            if initial_value == "use-last" {
                let consumer = fluvio
                    .partition_consumer(self.fluvio_topic.clone(), self.fluvio_partition)
                    .await?;
                let stream = consumer.stream(fluvio::Offset::from_end(1)).await?;
                let timeout = stream.timeout(Duration::from_millis(3000));
                tokio::pin!(timeout);
                let last_record = StreamExt::try_next(&mut timeout)
                    .await
                    .ok()
                    .flatten()
                    .transpose();

                if let Ok(Some(record)) = last_record {
                    Ok(record.value().to_vec())
                } else {
                    Ok(Vec::new())
                }
            } else {
                Ok(initial_value.as_bytes().to_vec())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub async fn get_smartmodule(&self, name: &str, fluvio: &Fluvio) -> anyhow::Result<Vec<u8>> {
        use flate2::bufread::GzDecoder;
        use std::io::Read;

        match std::fs::read(name) {
            Ok(data) => Ok(data),
            Err(_) => {
                let admin = fluvio.admin().await;

                let smartmodule_spec_list =
                    &admin.list::<SmartModuleSpec, _>(vec![name.into()]).await?;

                let smartmodule_spec = &smartmodule_spec_list
                    .first()
                    .context("Not found smartmodule")?
                    .spec;

                let mut decoder = GzDecoder::new(&*smartmodule_spec.wasm.payload);
                let mut buffer = Vec::with_capacity(smartmodule_spec.wasm.payload.len());
                decoder.read_to_end(&mut buffer)?;
                Ok(buffer)
            }
        }
    }

    async fn create_consumer(&self) -> anyhow::Result<PartitionConsumer> {
        self.ensure_topic_exists().await?;
        Ok(fluvio::consumer(&self.fluvio_topic, self.fluvio_partition).await?)
    }

    pub async fn create_consumer_stream(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<Record, ErrorCode>>> {
        let fluvio = fluvio::Fluvio::connect().await?;
        let wasm_invocation: Option<SmartModuleInvocation> =
            match (&self.filter, &self.map, &self.arraymap, &self.filter_map) {
                (Some(filter_path), _, _, _) => {
                    let data = self.get_smartmodule(filter_path, &fluvio).await?;
                    Some(SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::adhoc_from_bytes(&data)?,
                        kind: SmartModuleKind::Filter,
                        params: Default::default(),
                    })
                }
                (_, Some(map_path), _, _) => {
                    let data = self.get_smartmodule(map_path, &fluvio).await?;
                    Some(SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::adhoc_from_bytes(&data)?,
                        kind: SmartModuleKind::Map,
                        params: Default::default(),
                    })
                }
                (_, _, Some(array_map_path), _) => {
                    let data = self.get_smartmodule(array_map_path, &fluvio).await?;
                    Some(SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::adhoc_from_bytes(&data)?,
                        kind: SmartModuleKind::ArrayMap,
                        params: Default::default(),
                    })
                }
                (_, _, _, Some(filter_map_path)) => {
                    let data = self.get_smartmodule(filter_map_path, &fluvio).await?;
                    Some(SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::adhoc_from_bytes(&data)?,
                        kind: SmartModuleKind::FilterMap,
                        params: Default::default(),
                    })
                }
                _ => None,
            };
        let mut builder = ConsumerConfig::builder();
        builder.smartmodule(wasm_invocation);
        let config = builder.build()?;
        let consumer = self.create_consumer().await?;
        let offset = fluvio::Offset::end();
        Ok(consumer.stream_with_config(offset, config).await?)
    }
}
