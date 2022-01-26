use anyhow::Context;
use schemars::JsonSchema;
use structopt::clap::AppSettings;
use structopt::StructOpt;

pub use fluvio::{consumer, Fluvio, PartitionConsumer, TopicProducer};

use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;

#[derive(StructOpt, Debug, JsonSchema, Clone)]
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
    pub async fn create_producer(&self) -> anyhow::Result<TopicProducer> {
        let fluvio = fluvio::Fluvio::connect().await?;

        let producer = match (&self.filter, &self.map, &self.arraymap) {
            (Some(filter_path), _, _) => {
                let data = self.get_smartmodule(filter_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_filter(data, Default::default())?
            }
            (_, Some(map_path), _) => {
                let data = self.get_smartmodule(map_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_map(data, Default::default())?
            }
            (_, _, Some(array_map_path)) => {
                let data = self.get_smartmodule(array_map_path, &fluvio).await?;
                fluvio
                    .topic_producer(&self.fluvio_topic)
                    .await?
                    .with_array_map(data, Default::default())?
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

    pub async fn create_consumer(
        &self,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<PartitionConsumer> {
        let consumer = consumer(topic, partition).await?;
        Ok(consumer)
    }
}
