// src/storage/filesystem.rs
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::fs;
use std::path::{Path, PathBuf};

use super::StorageManager;

#[derive(Debug)]
pub struct FilesystemStorage {
    cache_base: PathBuf,
}

impl FilesystemStorage {
    pub fn new(cache_base: &str) -> Self {
        Self {
            cache_base: PathBuf::from(cache_base),
        }
    }
}

#[async_trait]
impl StorageManager for FilesystemStorage {
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        // Return directory path (DLT creates <table>/*.parquet files)
        // This matches S3Storage behavior and works with DataFusion's ListingTable
        let path = self
            .cache_base
            .join(connection_id.to_string())
            .join(schema)
            .join(table);
        format!("file://{}", path.display())
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
        self.cache_base
            .join(connection_id.to_string())
            .to_string_lossy()
            .to_string()
    }

    async fn read(&self, url: &str) -> Result<Vec<u8>> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        Ok(fs::read(path)?)
    }

    async fn write(&self, url: &str, data: &[u8]) -> Result<()> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        let path = Path::new(path);

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(path, data)?;
        Ok(())
    }

    async fn delete(&self, url: &str) -> Result<()> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        let path = Path::new(path);

        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(path)?;
            } else {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let path = Path::new(prefix);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        let path = url
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid file URL: {}", url))?;
        Ok(Path::new(path).exists())
    }

    fn register_with_datafusion(&self, _ctx: &SessionContext) -> Result<()> {
        // No-op for filesystem - DataFusion handles file:// by default
        Ok(())
    }

    fn prepare_cache_write(&self, connection_id: i32, schema: &str, table: &str) -> PathBuf {
        // Write file INSIDE the table directory (for ListingTable compatibility)
        self.cache_base
            .join(connection_id.to_string())
            .join(schema)
            .join(table) // table directory
            .join(format!("{}.parquet", table)) // file inside directory
    }

    fn prepare_versioned_cache_write(
        &self,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> PathBuf {
        // Use versioned DIRECTORIES to avoid duplicate reads during grace period.
        // DataFusion's ListingTable reads all parquet files in a directory.
        // By using versioned directories with a fixed filename, we ensure only
        // the active version is read after catalog update.
        // Path: {cache_base}/{conn_id}/{schema}/{table}/{version}/data.parquet
        let version = nanoid::nanoid!(8);
        self.cache_base
            .join(connection_id.to_string())
            .join(schema)
            .join(table)
            .join(version)
            .join("data.parquet")
    }

    async fn finalize_cache_write(
        &self,
        written_path: &Path,
        _connection_id: i32,
        _schema: &str,
        _table: &str,
    ) -> Result<String> {
        // For local storage, the file is already in place.
        // Return the parent directory URL (for ListingTable compatibility).
        // For versioned writes: written_path is {version}/data.parquet,
        // so parent is the version directory which contains only this file.
        let parent = written_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Written path has no parent"))?;
        Ok(format!("file://{}", parent.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versioned_cache_path_unique() {
        let storage = FilesystemStorage::new("/tmp/cache");
        let path1 = storage.prepare_versioned_cache_write(1, "main", "orders");
        let path2 = storage.prepare_versioned_cache_write(1, "main", "orders");
        assert_ne!(path1, path2, "Versioned paths should be unique");
    }

    #[test]
    fn test_versioned_cache_path_structure() {
        // New structure: {cache_base}/{conn_id}/{schema}/{table}/{version}/data.parquet
        let storage = FilesystemStorage::new("/tmp/cache");
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
        // The version is a directory, not a filename suffix
        assert!(
            path_str.ends_with("/data.parquet"),
            "Path should end with /data.parquet, got: {}",
            path_str
        );
    }

    #[test]
    fn test_versioned_directories_are_separate() {
        // Verify that each version gets its own isolated directory
        let storage = FilesystemStorage::new("/tmp/cache");
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
}
