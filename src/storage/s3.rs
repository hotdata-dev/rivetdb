// src/storage/s3.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;
use url::Url;

use super::{S3Credentials, StorageManager};

#[derive(Debug, Clone)]
struct S3Config {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

#[derive(Debug)]
pub struct S3Storage {
    bucket: String,
    store: Arc<dyn ObjectStore>,
    config: Option<S3Config>,
}

impl S3Storage {
    pub fn new(bucket: &str) -> Result<Self> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()?;

        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
            config: None,
        })
    }

    /// Create S3Storage with custom endpoint for MinIO/S3-compatible storage
    pub fn new_with_config(
        bucket: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        allow_http: bool,
    ) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(allow_http);

        // For MinIO, we need to use path-style URLs
        builder = builder.with_virtual_hosted_style_request(false);

        let store = builder.build()?;

        Ok(Self {
            bucket: bucket.to_string(),
            store: Arc::new(store),
            config: Some(S3Config {
                endpoint: endpoint.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
            }),
        })
    }

    fn url_to_path(&self, url: &str) -> Result<ObjectPath> {
        // Convert s3://bucket/path/to/file to ObjectPath
        let url = Url::parse(url)?;
        let path = url.path().trim_start_matches('/');
        Ok(ObjectPath::from(path))
    }
}

#[async_trait]
impl StorageManager for S3Storage {
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        // For S3, return directory path (DLT creates <table>/*.parquet files)
        format!(
            "s3://{}/cache/{}/{}/{}",
            self.bucket, connection_id, schema, table
        )
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
        format!("s3://{}/cache/{}", self.bucket, connection_id)
    }

    async fn read(&self, url: &str) -> Result<Vec<u8>> {
        let path = self.url_to_path(url)?;
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes.to_vec())
    }

    async fn write(&self, url: &str, data: &[u8]) -> Result<()> {
        let path = self.url_to_path(url)?;
        self.store.put(&path, data.to_vec().into()).await?;
        Ok(())
    }

    async fn delete(&self, url: &str) -> Result<()> {
        let path = self.url_to_path(url)?;
        self.store.delete(&path).await?;
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let url = Url::parse(prefix)?;
        let prefix_path = url.path().trim_start_matches('/');
        let prefix_path = ObjectPath::from(prefix_path);

        // List all objects with prefix and delete them
        let list = self.store.list(Some(&prefix_path));
        let objects: Vec<_> = list.try_collect().await?;

        for obj in objects {
            self.store.delete(&obj.location).await?;
        }
        Ok(())
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        let path = self.url_to_path(url)?;
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()> {
        let url = Url::parse(&format!("s3://{}", self.bucket))?;
        ctx.runtime_env()
            .register_object_store(&url, self.store.clone());
        Ok(())
    }

    fn get_s3_credentials(&self) -> Option<S3Credentials> {
        self.config.as_ref().map(|c| S3Credentials {
            aws_access_key_id: c.access_key.clone(),
            aws_secret_access_key: c.secret_key.clone(),
            endpoint_url: c.endpoint.clone(),
        })
    }

    fn prepare_cache_write(
        &self,
        _connection_id: i32,
        _schema: &str,
        table: &str,
    ) -> std::path::PathBuf {
        // Temp file path - will be uploaded to correct S3 location
        std::env::temp_dir().join(format!("{}-{}.parquet", table, uuid::Uuid::new_v4()))
    }

    fn prepare_versioned_cache_write(
        &self,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> std::path::PathBuf {
        // Use versioned DIRECTORIES to avoid duplicate reads during grace period.
        // Path structure: {temp_dir}/{conn_id}/{schema}/{table}/{version}/data.parquet
        // This matches the S3 destination structure for consistency.
        let version = nanoid::nanoid!(8);
        std::env::temp_dir()
            .join(connection_id.to_string())
            .join(schema)
            .join(table)
            .join(version)
            .join("data.parquet")
    }

    async fn finalize_cache_write(
        &self,
        written_path: &std::path::Path,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> Result<String> {
        let data = std::fs::read(written_path)?;

        // Determine if this is a versioned write by checking path structure.
        // Versioned writes have: {temp}/{conn}/{schema}/{table}/{version}/data.parquet
        // Non-versioned have: {temp}/{table}-{uuid}.parquet
        let filename = written_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        let is_versioned = filename == "data.parquet";

        let (file_url, dir_url) = if is_versioned {
            // For versioned writes, extract version from path and create versioned S3 path
            // Local path: {temp}/{conn}/{schema}/{table}/{version}/data.parquet
            // S3 path: s3://bucket/cache/{conn}/{schema}/{table}/{version}/data.parquet
            let version_dir = written_path.parent().and_then(|p| p.file_name());
            let version = version_dir
                .and_then(|v| v.to_str())
                .ok_or_else(|| anyhow::anyhow!("Could not extract version from path"))?;

            let versioned_dir_url = format!(
                "s3://{}/cache/{}/{}/{}/{}",
                self.bucket, connection_id, schema, table, version
            );
            let file_url = format!("{}/data.parquet", versioned_dir_url);
            (file_url, versioned_dir_url)
        } else {
            // Non-versioned write: use the standard cache_url directory
            let dir_url = self.cache_url(connection_id, schema, table);
            let file_url = format!("{}/{}.parquet", dir_url, table);
            (file_url, dir_url)
        };

        self.write(&file_url, &data).await?;
        std::fs::remove_file(written_path)?;

        Ok(dir_url) // Return directory URL for ListingTable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versioned_cache_path_unique() {
        // S3Storage uses temp dir for local writes before upload
        // Create a mock S3Storage just for path generation testing
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: None,
        };

        let path1 = storage.prepare_versioned_cache_write(1, "main", "orders");
        let path2 = storage.prepare_versioned_cache_write(1, "main", "orders");
        assert_ne!(path1, path2, "Versioned paths should be unique");
    }

    #[test]
    fn test_versioned_cache_path_structure() {
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: None,
        };

        let path = storage.prepare_versioned_cache_write(42, "public", "users");
        let path_str = path.to_string_lossy();

        assert!(
            path_str.contains("/42/"),
            "Path should contain connection_id"
        );
        assert!(path_str.contains("/public/"), "Path should contain schema");
        assert!(
            path_str.contains("/users/"),
            "Path should contain table directory"
        );
        assert!(
            path_str.ends_with("/data.parquet"),
            "Path should end with /data.parquet, got: {}",
            path_str
        );
    }

    #[test]
    fn test_versioned_directories_are_separate() {
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: Arc::new(object_store::memory::InMemory::new()),
            config: None,
        };

        let path1 = storage.prepare_versioned_cache_write(1, "main", "orders");
        let path2 = storage.prepare_versioned_cache_write(1, "main", "orders");

        // Both should end with data.parquet
        assert!(path1.ends_with("data.parquet"));
        assert!(path2.ends_with("data.parquet"));

        // But their parent directories (version dirs) should be different
        let dir1 = path1.parent().unwrap();
        let dir2 = path2.parent().unwrap();
        assert_ne!(dir1, dir2, "Version directories should be different");

        // And both should be under the same table directory
        let table_dir1 = dir1.parent().unwrap();
        let table_dir2 = dir2.parent().unwrap();
        assert_eq!(
            table_dir1, table_dir2,
            "Both should be under same table dir"
        );
    }

    #[test]
    fn test_extract_version_from_path() {
        // Test that we can correctly extract version from the local temp path
        // This simulates what finalize_cache_write does
        let temp_dir = std::env::temp_dir();
        let version = "abc12345";
        let path = temp_dir
            .join("1")
            .join("public")
            .join("users")
            .join(version)
            .join("data.parquet");

        let version_dir = path.parent().and_then(|p| p.file_name());
        let extracted = version_dir.and_then(|v| v.to_str()).unwrap();
        assert_eq!(extracted, version, "Should extract version from path");
    }

    #[test]
    fn test_versioned_s3_url_format() {
        // Test that the S3 URL is correctly formatted with version
        let bucket = "my-bucket";
        let connection_id = 42;
        let schema = "public";
        let table = "users";
        let version = "abc12345";

        let versioned_dir_url = format!(
            "s3://{}/cache/{}/{}/{}/{}",
            bucket, connection_id, schema, table, version
        );
        let file_url = format!("{}/data.parquet", versioned_dir_url);

        assert_eq!(
            versioned_dir_url,
            "s3://my-bucket/cache/42/public/users/abc12345"
        );
        assert_eq!(
            file_url,
            "s3://my-bucket/cache/42/public/users/abc12345/data.parquet"
        );
    }

    #[tokio::test]
    async fn test_finalize_versioned_write_uses_version_in_s3_path() {
        // This test verifies that finalize_cache_write extracts the version
        // from the local path and uses it in the S3 destination URL.
        // We use in-memory object store to avoid real S3 calls.
        let store = Arc::new(object_store::memory::InMemory::new());
        let storage = S3Storage {
            bucket: "test-bucket".to_string(),
            store: store.clone(),
            config: None,
        };

        // Create a temp file that mimics versioned path structure
        let version = "testver1";
        let temp_base = std::env::temp_dir().join("s3_test_finalize");
        let versioned_dir = temp_base
            .join("1")
            .join("public")
            .join("orders")
            .join(version);
        std::fs::create_dir_all(&versioned_dir).unwrap();

        let temp_file = versioned_dir.join("data.parquet");
        std::fs::write(&temp_file, b"test parquet data").unwrap();

        // Call finalize_cache_write
        let result_url = storage
            .finalize_cache_write(&temp_file, 1, "public", "orders")
            .await
            .unwrap();

        // Verify the returned URL contains the version
        assert!(
            result_url.contains(version),
            "Result URL should contain version: {}",
            result_url
        );
        assert_eq!(
            result_url,
            format!("s3://test-bucket/cache/1/public/orders/{}", version)
        );

        // Verify the file was uploaded to the correct S3 path
        let s3_path = ObjectPath::from(format!("cache/1/public/orders/{}/data.parquet", version));
        let result = store.get(&s3_path).await;
        assert!(result.is_ok(), "File should exist at versioned S3 path");

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_base);
    }
}
