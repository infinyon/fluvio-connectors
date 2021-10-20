use serde::Deserialize;
use std::collections::HashMap;

pub type ConnectorConfigSet = HashMap<String, ConnectorConfig>;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    /// The type of connector used
    r#type: String,
    /// The topic for this connector. If none, defaults to `name`
    topic: Option<String>,
    /// Auto create topic if it doesn't exist
    #[serde(default = "ConnectorConfig::create_topic_default")]
    create_topic: bool,
    /// Specefic arguments to the connector itself. Such as API Keys, etc.
    inputs: Option<HashMap<String, String>>,
}

impl ConnectorConfig {
    pub fn create_topic_default() -> bool {
        true
    }
}

#[cfg(test)]
mod yaml {
    use super::*;
    #[test]
    fn test_yaml() {
        let input_yaml = "#
my-simple-connector:
  type: syslog

my-complex-connector:
  type: syslog
  version: 0.1
  topic_name: my-syslog-connector # defaults to id
  inputs:
    bind_url: tcp://0.0.0.0:9000
#";
        let config: ConnectorConfigSet =
            serde_yaml::from_str(input_yaml).expect("Failed to parse yaml");
    }
}
