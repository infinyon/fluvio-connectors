use crate::opt::TransformOpt;
use fluvio::dataplane::record::Record;
use fluvio_smartengine::{SmartEngine, SmartModuleChainInstance, SmartModuleConfig};

use fluvio::Fluvio;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

#[derive(Debug)]
pub struct Transformations {
    smart_module_chain: SmartModuleChainInstance,
}

impl Transformations {
    // TODO: move up to common crate
    pub async fn from_fluvio(value: Vec<TransformOpt>) -> anyhow::Result<Transformations> {
        let mut builder = SmartEngine::new().builder();
        if value.is_empty() {
            return Ok(Self {
                smart_module_chain: builder.initialize()?,
            });
        }

        let fluvio = Fluvio::connect().await?;

        for step in value {
            let raw = CommonConnectorOpt::default()
                .get_smartmodule(&step.uses, &fluvio)
                .await?;

            let config = SmartModuleConfig::builder()
                .params(step.with.into())
                .build()?;
            builder.add_smart_module(config, raw)?;
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
