use anyhow::Context;
use schemars::JsonSchema;
use structopt::StructOpt;

use fluvio::{Fluvio, SmartStreamConfig, TopicProducer};

use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;

#[derive(StructOpt, Debug, JsonSchema)]
pub struct CommonSourceOpt {
    /// The topic where all things should go
    #[structopt(long)]
    #[schemars(skip)]
    pub fluvio_topic: String,

    /// The rust log level.
    #[structopt(long)]
    #[schemars(skip)]
    pub rust_log: Option<String>,

    /// Path of filter smartstream used as a pre-produce step
    /// If not found file in that path, it will be fetch
    /// the smartmodule with that name if present
    #[structopt(long, group("smartstream"))]
    pub smartstream_filter: Option<String>,

    /// Path of map smartstream used as a pre-produce step
    /// If not found file in that path, it will be fetch
    /// the smartmodule with that name if present
    #[structopt(long, group("smartstream"))]
    pub smartstream_map: Option<String>,

    /// Path of arraymap smartstream used as a pre-produce step
    /// If not found file in that path, it will be fetch
    /// the smartmodule with that name if present
    #[structopt(long, group("smartstream"))]
    pub smartstream_arraymap: Option<String>,
}

impl CommonSourceOpt {
    pub fn enable_logging(&self) {
        if let Some(ref rust_log) = self.rust_log {
            std::env::set_var("RUST_LOG", rust_log);
        }
        fluvio_future::subscriber::init_logger();
    }
    pub async fn create_producer(&self) -> anyhow::Result<TopicProducer> {
        let fluvio = fluvio::Fluvio::connect().await?;
        let mut producer = fluvio.topic_producer(&self.fluvio_topic).await?;
        let smartstream_config: &mut _ = producer.get_smartstream_mut();

        let config = match (
            &self.smartstream_filter,
            &self.smartstream_map,
            &self.smartstream_arraymap,
        ) {
            (Some(filter_path), _, _) => {
                let data = self.get_smartmodule(filter_path, &fluvio).await?;
                let config = SmartStreamConfig::default();
                Some(config.wasm_filter(data, Default::default()))
            }
            (_, Some(map_path), _) => {
                let data = self.get_smartmodule(map_path, &fluvio).await?;
                let config = SmartStreamConfig::default();
                Some(config.wasm_map(data, Default::default()))
            }
            (_, _, Some(array_map_path)) => {
                let data = self.get_smartmodule(array_map_path, &fluvio).await?;
                let config = SmartStreamConfig::default();
                Some(config.wasm_array_map(data, Default::default()))
            }
            _ => None,
        };

        if let Some(smart_config) = config {
            *smartstream_config = smart_config;
        }
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
}
