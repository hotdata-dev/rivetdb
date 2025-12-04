// src/storage/mod.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub mod filesystem;
pub mod s3;

// Re-exports
pub use filesystem::FilesystemStorage;
pub use s3::S3Storage;

/// S3 credentials for passing to sync scripts (e.g., DLT).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credentials {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub endpoint_url: String,
}

#[async_trait]
pub trait StorageManager: Debug + Send + Sync {
    // Path construction
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String;
    fn state_url(&self, connection_id: i32, schema: &str, table: &str) -> String;
    fn cache_prefix(&self, connection_id: i32) -> String;
    fn state_prefix(&self, connection_id: i32) -> String;

    // File operations
    async fn read(&self, url: &str) -> Result<Vec<u8>>;
    async fn write(&self, url: &str, data: &[u8]) -> Result<()>;
    async fn delete(&self, url: &str) -> Result<()>;
    async fn delete_prefix(&self, prefix: &str) -> Result<()>;
    async fn exists(&self, url: &str) -> Result<bool>;

    // DataFusion integration
    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()>;

    /// Get S3 credentials for DLT sync script.
    /// Returns None for non-S3 storage backends.
    fn get_s3_credentials(&self) -> Option<S3Credentials> {
        None
    }
}
