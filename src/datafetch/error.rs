use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataFetchError {
    #[error("driver load failed: {0}")]
    DriverLoad(String),

    #[error("connection failed: {0}")]
    Connection(String),

    #[error("query failed: {0}")]
    Query(String),

    #[error("storage write failed: {0}")]
    Storage(String),

    #[error("unsupported driver: {0}")]
    UnsupportedDriver(String),

    #[error("discovery failed: {0}")]
    Discovery(String),

    #[error("schema serialization failed: {0}")]
    SchemaSerialization(String),
}

impl From<std::io::Error> for DataFetchError {
    fn from(e: std::io::Error) -> Self {
        DataFetchError::Storage(e.to_string())
    }
}