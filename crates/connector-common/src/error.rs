#[derive(thiserror::Error, Debug)]
pub enum ConnectorLoadError {
    #[error("Invalid yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("ByteSize: {0}")]
    ByteSizeParse(String),
}
