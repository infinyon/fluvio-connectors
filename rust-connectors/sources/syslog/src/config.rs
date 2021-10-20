use serde::Deserialize;
use std::convert::TryFrom;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    source: ConnectorSource,
}

#[derive(Debug, Deserialize)]
pub struct ConnectorSource {
    name: String,
    r#type: String,
    filter_prefix: Option<String>,
    topic: Option<String>,
    create_topic: Option<bool>,
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Io Error {0}")]
    IoError(#[from] std::io::Error),

    #[error("Toml Error {0}")]
    TomlError(#[from] toml::de::Error),
}

impl TryFrom<&Path> for ConnectorConfig {
    type Error = ConfigError;
    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        use std::fs::File;
        use std::io::Read;
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: ConnectorConfig = toml::from_str(&contents)?;
        Ok(config)
    }
}
