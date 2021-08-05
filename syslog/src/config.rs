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
        use std::io::Read;
        use std::fs::File;
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config : ConnectorConfig = toml::from_str(&contents)?;
        Ok(config)
    }
}



#[cfg(test)]
mod connector_tests {
    use super::*;
    use std::io::Read;
    use std::fs::File;
    #[test]
    fn test_parsing() {
        let mut file = File::open("connector.toml").expect("Failed to open file");
        let mut contents = String::new();
        file.read_to_string(&mut contents).expect("Failed to read contents");
        let _config : ConnectorConfig = toml::from_str(&contents).expect("Failed to parse toml");
    }

    #[test]
    fn test_path() {
        let _config = ConnectorConfig::try_from(Path::new("connector.toml")).expect("Failed to get config from file");
    }
}
