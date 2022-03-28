use anyhow::Context;
use schemars::JsonSchema;
use structopt::clap::AppSettings;
use structopt::StructOpt;

pub use fluvio::{
    consumer,
    consumer::{Record, SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind},
    dataplane::ErrorCode,
    metadata::topic::TopicSpec,
    ConsumerConfig, Fluvio, FluvioError, PartitionConsumer, TopicProducer,
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

    #[structopt(long, group("smartmodule"))]
    pub aggregate_init: Option<String>,
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

        let producer = match (
            &self.filter,
            &self.filter_map,
            &self.map,
            &self.arraymap,
            &self.aggregate,
        ) {
            (Some(filter_path), _, _, _, _) => {
                let data = self.get_smartmodule(filter_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_filter(data, Default::default())?
            }
            (_, Some(filter_map_path), _, _, _) => {
                let data = self.get_smartmodule(filter_map_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_filter(data, Default::default())?
            }
            (_, _, Some(map_path), _, _) => {
                let data = self.get_smartmodule(map_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_map(data, Default::default())?
            }
            (_, _, _, Some(array_map_path), _) => {
                let data = self.get_smartmodule(array_map_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_array_map(data, Default::default())?
            }
            (_, _, _, _, Some(aggregate)) => {
                let data = self.get_smartmodule(aggregate, &fluvio).await?;
                let initial = self.aggregate_init.clone().unwrap_or_default();
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_aggregate(data, Default::default(), initial.as_bytes().to_vec())?
            }
            _ => fluvio.topic_producer(&self.fluvio_topic).await?,
        };

        Ok(producer)
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

    async fn create_consumer(&self, partition: i32) -> anyhow::Result<PartitionConsumer> {
        self.ensure_topic_exists().await?;
        Ok(fluvio::consumer(&self.fluvio_topic, partition).await?)
    }

    pub async fn create_consumer_stream(
        &self,
        partition: i32,
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
        let consumer = self.create_consumer(partition).await?;
        let offset = fluvio::Offset::end();
        Ok(consumer.stream_with_config(offset, config).await?)
    }
}
