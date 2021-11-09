use fluvio_smartengine::{SmartEngine, SmartStream};
use schemars::JsonSchema;
use structopt::StructOpt;

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
    pub fn smartmodule_name(&self) -> Option<&str> {
        match (
            &self.smartstream_filter,
            &self.smartstream_map,
            &self.smartstream_arraymap,
        ) {
            (Some(filter_path), _, _) => Some(filter_path),
            (_, Some(map_path), _) => Some(map_path),
            (_, _, Some(array_map_path)) => Some(array_map_path),
            _ => None,
        }
    }
    pub fn smarstream_module(&self) -> anyhow::Result<Option<Box<dyn SmartStream>>> {
        let engine = SmartEngine::default();

        let smartmodule: Option<Box<dyn SmartStream>> = match (
            &self.smartstream_filter,
            &self.smartstream_map,
            &self.smartstream_arraymap,
        ) {
            (Some(filter_path), _, _) => {
                let smart_stream_module = engine.create_module_from_path(filter_path)?;
                Some(Box::new(
                    smart_stream_module.create_filter(&engine, Default::default())?,
                ))
            }
            (_, Some(map_path), _) => {
                let smart_stream_module = engine.create_module_from_path(map_path)?;
                Some(Box::new(
                    smart_stream_module.create_map(&engine, Default::default())?,
                ))
            }
            (_, _, Some(array_map_path)) => {
                let smart_stream_module = engine.create_module_from_path(&array_map_path)?;
                Some(Box::new(
                    smart_stream_module.create_array_map(&engine, Default::default())?,
                ))
            }
            _ => None,
        };
        Ok(smartmodule)
    }

    pub async fn smart_stream_module_from_spec(
        &self,
        smartmodule_spec: &SmartModuleSpec,
    ) -> anyhow::Result<Option<Box<dyn SmartStream>>> {
        let engine = SmartEngine::default();
        let smart_stream_module = engine
            .create_module_from_smartmodule_spec(smartmodule_spec)
            .await?;

        let smartmodule: Option<Box<dyn SmartStream>> = match (
            &self.smartstream_filter,
            &self.smartstream_map,
            &self.smartstream_arraymap,
        ) {
            (Some(_), _, _) => Some(Box::new(
                smart_stream_module.create_filter(&engine, Default::default())?,
            )),
            (_, Some(_), _) => Some(Box::new(
                smart_stream_module.create_map(&engine, Default::default())?,
            )),
            (_, _, Some(_)) => Some(Box::new(
                smart_stream_module.create_array_map(&engine, Default::default())?,
            )),
            _ => None,
        };
        Ok(smartmodule)
    }
}
