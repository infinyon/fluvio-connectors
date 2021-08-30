
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    r#type: String,
    topic: Option<String>,
    create_topic: Option<bool>,
}
