use crate::download::Downloader;
use crate::opt::TransformOpt;
use fluvio::dataplane::record::Record;
use fluvio_smartengine::engine::{SmartEngine, SmartModuleInstance};
use fluvio_smartengine::metadata::{
    LegacySmartModulePayload, SmartModuleKind, SmartModuleWasmCompressed,
};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;
use std::collections::BTreeMap;
use url::Url;

const PARAM_WITH: &str = "with";

pub struct Transformations {
    smart_modules: Vec<Box<dyn SmartModuleInstance>>,
}

impl Transformations {
    pub async fn from_hub(
        hub_url: Url,
        value: Vec<TransformOpt>,
    ) -> anyhow::Result<Transformations> {
        let mut smart_modules = Vec::with_capacity(value.len());
        if value.is_empty() {
            return Ok(Self { smart_modules });
        }
        let downloader = Downloader::from_url(hub_url)?;
        for step in value {
            let mut param: BTreeMap<String, String> = BTreeMap::new();
            if let Some(with) = step.with {
                param.insert(PARAM_WITH.to_string(), with);
            }
            let raw = downloader.download_binary(step.uses.as_str()).await?;
            let payload = LegacySmartModulePayload {
                wasm: SmartModuleWasmCompressed::Raw(raw),
                kind: SmartModuleKind::Map,
                params: param.into(),
            };
            let engine = SmartEngine::default();
            let mut smart_module = engine.create_module_from_payload(payload, None)?;
            smart_module.invoke_constructor()?;
            smart_modules.push(smart_module);
        }
        Ok(Self { smart_modules })
    }

    pub fn transform(&mut self, input: Record) -> anyhow::Result<Vec<Record>> {
        let mut result = vec![input];
        for smart_module in self.smart_modules.iter_mut() {
            let input = SmartModuleInput::try_from(result)?;
            let output = smart_module.process(input)?;
            if let Some(err) = output.error {
                return Err(err.into());
            }
            result = output.successes;
        }
        Ok(result)
    }

    pub fn size(&self) -> usize {
        self.smart_modules.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluvio_smartengine::engine::SmartModuleContext;
    use fluvio_smartmodule::dataplane::smartmodule::{
        SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput,
    };

    #[derive(Debug)]
    struct TestSmartModule(anyhow::Result<SmartModuleOutput>);

    impl SmartModuleInstance for TestSmartModule {
        fn process(&mut self, _input: SmartModuleInput) -> anyhow::Result<SmartModuleOutput> {
            Ok(SmartModuleOutput {
                successes: self.0.as_ref().unwrap().successes.clone(),
                error: None,
            })
        }

        fn params(&self) -> SmartModuleExtraParams {
            unimplemented!()
        }

        fn mut_ctx(&mut self) -> &mut SmartModuleContext {
            unimplemented!()
        }
    }

    #[test]
    fn test_transform_no_modules() {
        // given
        let mut transformations = Transformations {
            smart_modules: vec![],
        };
        let input = Record::from(("key", "value"));

        // when
        let result = transformations
            .transform(input)
            .expect("transformation succeeded");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!("value", result[0].value.as_str().expect("valid value"));
    }

    #[test]
    fn test_transform_one_module() {
        // given
        let output = SmartModuleOutput {
            successes: vec![Record::from(("key", "value"))],
            error: None,
        };
        let mut transformations = Transformations {
            smart_modules: vec![Box::new(TestSmartModule(Ok(output)))],
        };
        let input = Record::default();

        // when
        let result = transformations
            .transform(input)
            .expect("transformation succeeded");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!("value", result[0].value.as_str().expect("valid value"));
    }

    #[test]
    fn test_transform_many_modules_many_records() {
        // given
        let output1 = SmartModuleOutput {
            successes: vec![Record::from(("key", "1")), Record::from(("key", "1"))],
            error: None,
        };
        let output2 = SmartModuleOutput {
            successes: vec![Record::from(("key", "2")), Record::from(("key", "2"))],
            error: None,
        };
        let mut transformations = Transformations {
            smart_modules: vec![
                Box::new(TestSmartModule(Ok(output1))),
                Box::new(TestSmartModule(Ok(output2))),
            ],
        };
        let input = Record::default();

        // when
        let result = transformations
            .transform(input)
            .expect("transfromation succeeded");

        // then
        assert_eq!(result.len(), 2);
        assert_eq!("2", result[0].value.as_str().expect("valid value"));
    }
}
