use anyhow::Context;
use bytesize::ByteSize;
use fluvio_spu_schema::server::stream_fetch::SmartModuleContextData;
use humantime::parse_duration;
use schemars::{schema_for, JsonSchema};
use std::{collections::BTreeMap, time::Duration};
use structopt::clap::AppSettings;
use structopt::StructOpt;
use tokio_stream::Stream;

pub use fluvio::{
    consumer,
    consumer::{Record, SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind},
    dataplane::ErrorCode,
    metadata::smartmodule::SmartModuleSpec,
    metadata::topic::TopicSpec,
    Compression, ConsumerConfig, Fluvio, FluvioError, PartitionConsumer, TopicProducer,
    TopicProducerConfigBuilder,
};

use crate::error::CliError;

#[derive(StructOpt, Debug, JsonSchema, Clone, Default)]
#[structopt(settings = &[AppSettings::DeriveDisplayOrder])]
pub struct CommonConnectorOpt {
    /// The topic where this connector sends or receives records
    #[structopt(long)]
    #[schemars(skip)]
    pub fluvio_topic: String,

    /// The rust log level. If it is not defined, `RUST_LOG` environment variable
    /// will be used. If environment variable is not defined,
    /// then INFO level will be used.
    #[structopt(long)]
    pub rust_log: Option<String>,

    #[cfg(feature = "sink")]
    #[structopt(flatten)]
    #[schemars(flatten)]
    pub consumer_common: CommonConsumerOpt,

    #[cfg(feature = "source")]
    #[structopt(flatten)]
    #[schemars(flatten)]
    pub producer_common: CommonProducerOpt,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub smartmodule_common: CommonSmartModuleOpt,
}

#[derive(StructOpt, Debug, JsonSchema, Clone, Default)]
pub struct CommonConsumerOpt {
    #[structopt(long, default_value = "0")]
    pub consumer_partition: i32,
}

#[derive(StructOpt, Debug, JsonSchema, Clone, Default)]
pub struct CommonSmartModuleOpt {
    /// Path of filter smartmodule used as a pre-produce step
    /// if using source connector. If using sink connector this smartmodule
    /// will be used in consumer.
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule_group"))]
    pub filter: Option<String>,

    /// Path of filter_map smartmodule used as a pre-produce step
    /// if using source connector. If using sink connector this smartmodule
    /// will be used in consumer.
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule_group"))]
    pub filter_map: Option<String>,

    /// Path of map smartmodule used as a pre-produce step
    /// if using source connector. If using sink connector this smartmodule
    /// will be used in consumer.
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule_group"))]
    pub map: Option<String>,

    /// Path of arraymap smartmodule used as a pre-produce step
    /// if using source connector. If using sink connector this smartmodule
    /// will be used in consumer.
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("smartmodule_group"), alias = "arraymap")]
    pub array_map: Option<String>,

    /// Path of aggregate smartmodule used as a pre-produce step
    /// if using source connector. If using sink connector this smartmodule
    /// will be used in consumer.
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(long, group("aggregate_group"), group("smartmodule_group"))]
    pub aggregate: Option<String>,

    #[structopt(long, requires = "aggregate_group")]
    pub aggregate_initial_value: Option<String>,

    /// Path of smartmodule used as a pre-produce step
    /// if using source connector. If using sink connector this smartmodule
    /// will be used in consumer.    
    ///
    /// If the value is not a path to a file, it will be used
    /// to lookup a SmartModule by name
    #[structopt(
        long,
        alias = "smartmodule",
        group("aggregate_group"),
        group("smartmodule_group")
    )]
    pub smart_module: Option<String>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key:value format.
    ///
    /// It only accepts one key:value pair. In order to pass multiple pairs, call this option multiple times.
    ///
    /// Example:
    /// --smartmodule-parameters key1:value --smartmodule-parameters key2:value
    #[structopt(
        long,
        parse(try_from_str = parse_key_val),
        requires = "smartmodule_group", 
        number_of_values = 1
    )]
    pub smartmodule_parameters: Option<Vec<(String, String)>>,
}

fn parse_key_val(s: &str) -> Result<(String, String), CliError> {
    let pos = s.find(':').ok_or_else(|| {
        CliError::InvalidArg(format!("invalid KEY=value: no `:` found in `{}`", s))
    })?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
#[derive(StructOpt, Debug, JsonSchema, Clone, Default)]
pub struct CommonProducerOpt {
    /// Time to wait before sending
    /// Ex: '150ms', '20s'
    #[structopt(long, parse(try_from_str = parse_duration))]
    pub producer_linger: Option<Duration>,

    /// Compression algorithm to use when sending records.
    /// Supported values: none, gzip, snappy and lz4.
    #[structopt(long)]
    #[schemars(skip)]
    pub producer_compression: Option<Compression>,

    /// Max amount of bytes accumulated before sending
    #[structopt(long)]
    #[schemars(skip)]
    pub producer_batch_size: Option<ByteSize>,
}

#[cfg(feature = "source")]
impl CommonConnectorOpt {
    pub async fn create_producer(&self) -> anyhow::Result<TopicProducer> {
        let fluvio = fluvio::Fluvio::connect().await?;
        self.ensure_topic_exists().await?;
        let config_builder = TopicProducerConfigBuilder::default();
        let params = self.smartmodule_parameters();

        // Linger
        let config_builder = if let Some(linger) = self.producer_common.producer_linger {
            config_builder.linger(linger)
        } else {
            config_builder
        };

        // Compression
        let config_builder = if let Some(compression) = self.producer_common.producer_compression {
            config_builder.compression(compression)
        } else {
            config_builder
        };

        // Batch size
        let config_builder = if let Some(batch_size) = self.producer_common.producer_batch_size {
            config_builder.batch_size(batch_size.0 as usize)
        } else {
            config_builder
        };

        let config = config_builder.build()?;
        let producer = fluvio
            .topic_producer_with_config(&self.fluvio_topic, config)
            .await?;

        let producer = match (
            &self.smartmodule_common.smart_module,
            &self.smartmodule_common.filter,
            &self.smartmodule_common.filter_map,
            &self.smartmodule_common.map,
            &self.smartmodule_common.array_map,
            &self.smartmodule_common.aggregate,
        ) {
            (Some(smartmodule_name), _, _, _, _, _) => {
                let context = match &self.smartmodule_common.aggregate_initial_value {
                    Some(initial_value) => SmartModuleContextData::Aggregate {
                        accumulator: initial_value.as_bytes().to_vec(),
                    },
                    None => SmartModuleContextData::None,
                };

                let data = self.get_smartmodule(smartmodule_name, &fluvio).await?;
                producer.with_smartmodule(data, params, context)?
            }
            (_, Some(filter_path), _, _, _, _) => {
                let data = self.get_smartmodule(filter_path, &fluvio).await?;
                producer.with_filter(data, params)?
            }
            (_, _, Some(filter_map_path), _, _, _) => {
                let data = self.get_smartmodule(filter_map_path, &fluvio).await?;
                producer.with_filter_map(data, params)?
            }
            (_, _, _, Some(map_path), _, _) => {
                let data = self.get_smartmodule(map_path, &fluvio).await?;
                producer.with_map(data, params)?
            }
            (_, _, _, _, Some(array_map_path), _) => {
                let data = self.get_smartmodule(array_map_path, &fluvio).await?;
                producer.with_array_map(data, params)?
            }
            (_, _, _, _, _, Some(aggregate)) => {
                let data = self.get_smartmodule(aggregate, &fluvio).await?;
                let initial = self.get_aggregate_initial_value(&fluvio).await?;
                producer.with_aggregate(data, params, initial)?
            }
            _ => producer,
        };

        Ok(producer)
    }
    async fn get_aggregate_initial_value(&self, fluvio: &Fluvio) -> anyhow::Result<Vec<u8>> {
        use tokio_stream::StreamExt;
        if let Some(initial_value) = &self.smartmodule_common.aggregate_initial_value {
            if initial_value == "use-last" {
                let consumer = fluvio
                    .partition_consumer(self.fluvio_topic.clone(), 0)
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
}
#[cfg(feature = "sink")]
impl CommonConnectorOpt {
    pub async fn create_consumer(&self) -> anyhow::Result<PartitionConsumer> {
        self.ensure_topic_exists().await?;
        Ok(fluvio::consumer(&self.fluvio_topic, self.consumer_common.consumer_partition).await?)
    }

    pub async fn create_consumer_stream(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<Record, ErrorCode>>> {
        let fluvio = fluvio::Fluvio::connect().await?;
        let params = self.smartmodule_parameters().into();
        let wasm_invocation: Option<SmartModuleInvocation> = match (
            &self.smartmodule_common.smart_module,
            &self.smartmodule_common.filter,
            &self.smartmodule_common.map,
            &self.smartmodule_common.array_map,
            &self.smartmodule_common.filter_map,
            &self.smartmodule_common.aggregate,
        ) {
            (Some(smartmodule), _, _, _, _, _) => {
                let context = match &self.smartmodule_common.aggregate_initial_value {
                    Some(initial_value) => SmartModuleContextData::Aggregate {
                        accumulator: initial_value.as_bytes().to_vec(),
                    },
                    None => SmartModuleContextData::None,
                };
                Some(SmartModuleInvocation {
                    wasm: SmartModuleInvocationWasm::Predefined(smartmodule.to_owned()),
                    kind: SmartModuleKind::Generic(context),
                    params,
                })
            }
            (_, Some(filter_path), _, _, _, _) => Some(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::Predefined(filter_path.to_owned()),
                kind: SmartModuleKind::Filter,
                params,
            }),
            (_, _, Some(map_path), _, _, _) => Some(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::Predefined(map_path.to_owned()),
                kind: SmartModuleKind::Map,
                params,
            }),
            (_, _, _, Some(array_map_path), _, _) => Some(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::Predefined(array_map_path.to_owned()),
                kind: SmartModuleKind::ArrayMap,
                params,
            }),
            (_, _, _, _, Some(filter_map_path), _) => Some(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::Predefined(filter_map_path.to_owned()),
                kind: SmartModuleKind::FilterMap,
                params,
            }),
            (_, _, _, _, _, Some(aggregate_path)) => {
                let initial = self.get_aggregate_initial_value(&fluvio).await?;
                Some(SmartModuleInvocation {
                    wasm: SmartModuleInvocationWasm::Predefined(aggregate_path.to_owned()),
                    kind: SmartModuleKind::Aggregate {
                        accumulator: initial,
                    },
                    params,
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

impl CommonConnectorOpt {
    fn smartmodule_parameters(&self) -> BTreeMap<String, String> {
        self.smartmodule_common
            .smartmodule_parameters
            .clone()
            .map(|params| params.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn enable_logging(&self) {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info")
        }
        if let Some(ref rust_log) = self.rust_log {
            std::env::set_var("RUST_LOG", rust_log);
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
}

pub trait GetOpts {
    type Opt: StructOpt + JsonSchema;
    fn get_opt() -> Option<Self::Opt> {
        if let Some("metadata") = std::env::args().nth(1).as_deref() {
            let schema = schema_for!(Self::Opt);
            let metadata = serde_json::json!({
                "name": Self::name(),
                "version": Self::version(),
                "description": Self::description(),
                "schema": schema,
            });
            let metadata_json = serde_json::to_string_pretty(&metadata).unwrap();
            println!("{}", metadata_json);
            None
        } else {
            Some(Self::Opt::from_args())
        }
    }
    fn name() -> &'static str;
    fn version() -> &'static str;
    fn description() -> &'static str;
}
