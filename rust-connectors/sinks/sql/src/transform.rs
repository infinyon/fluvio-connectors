use crate::download::Downloader;
use crate::opt::TransformOpt;
use fluvio::dataplane::record::Record;
use fluvio_smartengine::{SmartEngine, SmartModuleChainInstance, SmartModuleConfig};

use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;
use std::collections::BTreeMap;
use fluvio::Fluvio;
use url::Url;
use fluvio_connectors_common::opt::CommonConnectorOpt;

const PARAM_WITH: &str = "with";

#[derive(Debug)]
pub struct Transformations {
    smart_module_chain: SmartModuleChainInstance,
}

impl Transformations {
    // TODO: move up to common crate and combine with from_fluvio method to allow mixed module sources
    pub async fn from_hub(
        hub_url: Url,
        value: Vec<TransformOpt>,
    ) -> anyhow::Result<Transformations> {
        let mut builder = SmartEngine::new().builder();
        if value.is_empty() {
            return Ok(Self {
                smart_module_chain: builder.initialize()?,
            });
        }

        let downloader = Downloader::from_url(hub_url)?;
        for step in value {
            let mut param: BTreeMap<String, String> = BTreeMap::new();
            if let Some(with) = step.with {
                param.insert(PARAM_WITH.to_string(), with);
            }
            let raw = downloader.download_binary(step.uses.as_str()).await?;
            let mut sm_builder = SmartModuleConfig::builder();
            let sm_builder = sm_builder.params(param.into());

            builder.add_smart_module(sm_builder.build()?, raw)?;
        }

        Ok(Self {
            smart_module_chain: builder.initialize()?,
        })
    }

    // TODO: move up to common crate
    pub async fn from_fluvio(
        value: Vec<TransformOpt>,
    ) -> anyhow::Result<Transformations> {
        let mut builder = SmartEngine::new().builder();
        if value.is_empty() {
            return Ok(Self {
                smart_module_chain: builder.initialize()?,
            });
        }

        let fluvio = Fluvio::connect().await?;

        for step in value {
            let mut param: BTreeMap<String, String> = BTreeMap::new();
            if let Some(with) = step.with {
                param.insert(PARAM_WITH.to_string(), with);
            }
            let raw = CommonConnectorOpt::default().get_smartmodule(&step.uses, &fluvio).await?;

            let mut sm_builder = SmartModuleConfig::builder();
            let sm_builder = sm_builder.params(param.into());

            builder.add_smart_module(sm_builder.build()?, raw)?;
        }

        Ok(Self {
            smart_module_chain: builder.initialize()?,
        })
    }

    pub fn transform(&mut self, input: Record) -> anyhow::Result<Vec<Record>> {
        let result = vec![input];
        let input = SmartModuleInput::try_from(result)?;
        let output = self.smart_module_chain.process(input)?;
        let result = output.successes;
        Ok(result)
    }
}
