use fluvio::dataplane::record::Record;
use fluvio_smartengine::{
    metrics::SmartModuleChainMetrics, SmartEngine, SmartModuleChainBuilder,
    SmartModuleChainInstance,
};

use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

#[derive(Debug)]
pub struct Transformations {
    smart_module_chain: SmartModuleChainInstance,
    metrics: SmartModuleChainMetrics,
}

impl Transformations {
    pub async fn from_chain(builder: SmartModuleChainBuilder) -> anyhow::Result<Transformations> {
        let engine = SmartEngine::new();
        Ok(Self {
            smart_module_chain: builder.initialize(&engine)?,
            metrics: SmartModuleChainMetrics::default(),
        })
    }

    pub fn transform(&mut self, input: Record) -> anyhow::Result<Vec<Record>> {
        let result = vec![input];
        let input = SmartModuleInput::try_from(result)?;
        let output = self.smart_module_chain.process(input, &self.metrics)?;
        let result = output.successes;
        Ok(result)
    }
}
