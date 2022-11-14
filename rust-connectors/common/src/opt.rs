use bytesize::ByteSize;
use clap::{AppSettings, Parser};
use fluvio_sc_schema::topic::TopicSpec;
use humantime::parse_duration;
use schemars::{schema_for, JsonSchema};
use std::str::FromStr;
use std::{collections::BTreeMap, time::Duration};

use fluvio::{
    Compression, FluvioConfig, SmartModuleContextData, SmartModuleExtraParams,
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind,
};
use serde::Deserialize;

#[derive(Parser, Debug, JsonSchema, Clone, Default)]
#[clap(settings = &[AppSettings::DeriveDisplayOrder])]
pub struct CommonConnectorOpt {
    /// The topic where this connector sends or receives records
    #[clap(long)]
    #[schemars(skip)]
    pub fluvio_topic: String,

    /// The rust log level. If it is not defined, `RUST_LOG` environment variable
    /// will be used. If environment variable is not defined,
    /// then INFO level will be used.
    #[clap(long)]
    pub rust_log: Option<String>,

    #[cfg(feature = "sink")]
    #[clap(flatten)]
    #[schemars(flatten)]
    pub consumer_common: CommonConsumerOpt,

    #[cfg(feature = "source")]
    #[clap(flatten)]
    #[schemars(flatten)]
    pub producer_common: CommonProducerOpt,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub transform_common: CommonTransformOpt,
}

#[derive(Parser, Debug, JsonSchema, Clone, Default)]
pub struct CommonConsumerOpt {
    #[clap(long, default_value = "0")]
    pub consumer_partition: i32,
}

#[derive(Parser, Debug, JsonSchema, Clone, Default)]
pub struct CommonProducerOpt {
    /// Time to wait before sending
    /// Ex: '150ms', '20s'
    #[clap(long, parse(try_from_str = parse_duration))]
    pub producer_linger: Option<Duration>,

    /// Compression algorithm to use when sending records.
    /// Supported values: none, gzip, snappy and lz4.
    #[clap(long)]
    #[schemars(skip)]
    pub producer_compression: Option<Compression>,

    /// Max amount of bytes accumulated before sending
    #[clap(long)]
    #[schemars(skip)]
    pub producer_batch_size: Option<ByteSize>,
}

#[cfg(feature = "source")]
impl CommonConnectorOpt {
    pub async fn create_producer(
        &self,
        connector_name: &str,
    ) -> anyhow::Result<fluvio::TopicProducer> {
        let mut cluster_config = FluvioConfig::load()?;
        cluster_config.client_id = Some(format!("fluvio_connector_{}", connector_name));

        let fluvio = fluvio::Fluvio::connect_with_config(&cluster_config).await?;
        self.ensure_topic_exists().await?;
        let config_builder = fluvio::TopicProducerConfigBuilder::default();

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

        if let Some(chain) = self.transform_common.create_smart_module_chain().await? {
            Ok(producer.with_chain(chain)?)
        } else {
            Ok(producer)
        }
    }
}
#[cfg(feature = "sink")]
impl CommonConnectorOpt {
    pub async fn create_consumer(&self) -> anyhow::Result<fluvio::PartitionConsumer> {
        self.ensure_topic_exists().await?;
        Ok(fluvio::consumer(&self.fluvio_topic, self.consumer_common.consumer_partition).await?)
    }

    pub async fn create_consumer_stream(
        &self,
        connector_name: &str,
    ) -> anyhow::Result<
        impl tokio_stream::Stream<
            Item = Result<fluvio::consumer::Record, fluvio_protocol::link::ErrorCode>,
        >,
    > {
        let mut cluster_config = FluvioConfig::load()?;
        cluster_config.client_id = Some(format!("fluvio_connector_{}", connector_name));
        let smartmodule = self
            .transform_common
            .transform
            .iter()
            .map(|t| t.into())
            .collect();
        let mut builder = fluvio::ConsumerConfig::builder();
        builder.smartmodule(smartmodule);
        let config = builder.build()?;
        let consumer = self.create_consumer().await?;
        let offset = fluvio::Offset::end();
        Ok(consumer.stream_with_config(offset, config).await?)
    }
}

impl CommonConnectorOpt {
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
        let topics = admin.list::<TopicSpec, String>(vec![]).await?;
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
}

#[derive(Parser, Debug, JsonSchema, Clone, Default)]
pub struct CommonTransformOpt {
    #[clap(long)]
    transform: Vec<TransformOpt>,
}

#[cfg(feature = "source")]
impl CommonTransformOpt {
    pub async fn create_smart_module_chain(
        &self,
    ) -> anyhow::Result<Option<fluvio::SmartModuleChainBuilder>> {
        use fluvio_sc_schema::smartmodule::SmartModuleApiClient;

        if self.transform.is_empty() {
            return Ok(None);
        }
        let api_client =
            SmartModuleApiClient::connect_with_config(FluvioConfig::load()?.try_into()?).await?;
        let mut builder = fluvio::SmartModuleChainBuilder::default();
        for step in &self.transform {
            let wasm = api_client
                .get(step.uses.clone())
                .await?
                .ok_or_else(|| anyhow::anyhow!("smartmodule {} not found", step.uses))?
                .wasm
                .as_raw_wasm()?;

            let config = fluvio::SmartModuleConfig::builder()
                .params(step.with.clone().into())
                .build()?;
            builder.add_smart_module(config, wasm);
        }

        Ok(Some(builder))
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[allow(dead_code)]
#[derive(Debug, JsonSchema, Clone, Deserialize)]
pub struct TransformOpt {
    pub(crate) uses: String,
    pub(crate) with: BTreeMap<String, String>,
}

impl FromStr for TransformOpt {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

impl From<&TransformOpt> for SmartModuleInvocation {
    fn from(opt: &TransformOpt) -> Self {
        SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::Predefined(opt.uses.clone()),
            kind: SmartModuleKind::Generic(SmartModuleContextData::None),
            params: SmartModuleExtraParams::from(opt.with.clone()),
        }
    }
}

pub trait GetOpts {
    type Opt: Parser + JsonSchema;
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
