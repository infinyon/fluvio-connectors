use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Parser, Debug, JsonSchema)]
pub(crate) struct MqttOpts {
    #[clap(long)]
    pub timeout: Option<u64>,

    #[clap(short, long, env = "MQTT_URL", hide_env_values = true)]
    pub mqtt_url: String,

    #[clap(long)]
    pub mqtt_topic: String,

    #[clap(long)]
    pub client_id: Option<String>,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,

    /// Record payload output type
    #[clap(long, default_value_t = Default::default())]
    pub payload_output_type: OutputType,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)] // The other variants aren't used but are part of the spec.
pub(crate) enum ConnectorDirection {
    Source,
    Sink,
}

#[derive(Parser, Debug, Default, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutputType {
    #[default]
    Binary,
    Json,
}

impl Display for OutputType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_value(self) {
            Ok(Value::String(s)) => write!(f, "{}", s),
            _ => Err(std::fmt::Error),
        }
    }
}

impl FromStr for OutputType {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_value(Value::String(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_type_parse() -> anyhow::Result<()> {
        //given
        let binary_type_value = "binary";
        let json_type_value = "json";

        //when
        let type1 = OutputType::from_str(binary_type_value)?;
        let type2 = OutputType::from_str(json_type_value)?;

        //then
        assert_eq!(type1.to_string(), binary_type_value);
        assert_eq!(type2.to_string(), json_type_value);
        Ok(())
    }
}
