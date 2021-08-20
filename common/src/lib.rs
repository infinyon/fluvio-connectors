use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfigSet {
    sources: Option<Vec<ConnectorConfig>>,
    sinks: Option<Vec<ConnectorConfig>>,
}

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    /// A unique identifier for this connector
    name: String,
    /// A scheme to a package to run. ex:
    /// * "file:://../test-connector"
    /// * "docker://infinyon/fluvio"
    package: String,
    /// The topic for this connector. If none, defaults to `name`
    topic: Option<String>,
    /// Auto create topic if it doesn't exist
    create_topic: Option<bool>,
    /// Specefic arguments to the connector itself. Such as API Keys, etc.
    connector_args: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod yaml{
    use super::*;
    #[test]
    fn test_yaml() {
        let input_yaml = "#
sources:
  - name: foo
    package: bar
  - name: baz
    package: bar

sinks:
  - name: foobar
    package: bar
#";
    let config : ConnectorConfigSet = serde_yaml::from_str(input_yaml).expect("Failed to parse yaml");
    assert!(config.sources.is_some());
    let sources = config.sources.unwrap();
    assert_eq!(sources[0].name, "foo");
    assert_eq!(sources[1].name, "baz");
    let sinks = config.sinks.unwrap();
    assert_eq!(sinks[0].name, "foobar");
    }
}
