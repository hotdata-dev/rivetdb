use anyhow::Result;
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use datafusion::prelude::SessionContext;
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_QUERY, PATH_RESULT};
use runtimedb::storage::{CacheWriteHandle, FilesystemStorage, StorageManager};
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

async fn setup_test() -> Result<(AppServer, TempDir)> {
    let temp_dir = tempfile::tempdir()?;

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);
    Ok((app, temp_dir))
}

/// A storage backend that delegates to FilesystemStorage but can be configured to fail.
#[derive(Debug)]
struct FailingStorage {
    inner: FilesystemStorage,
    fail_finalize: AtomicBool,
}

impl FailingStorage {
    fn new(base_dir: &std::path::Path) -> Self {
        Self {
            inner: FilesystemStorage::new(base_dir.to_str().expect("valid UTF-8 path")),
            fail_finalize: AtomicBool::new(false),
        }
    }

    fn set_fail_finalize(&self, should_fail: bool) {
        self.fail_finalize.store(should_fail, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageManager for FailingStorage {
    fn cache_url(&self, connection_id: i32, schema: &str, table: &str) -> String {
        self.inner.cache_url(connection_id, schema, table)
    }

    fn cache_prefix(&self, connection_id: i32) -> String {
        self.inner.cache_prefix(connection_id)
    }

    async fn read(&self, url: &str) -> Result<Vec<u8>> {
        self.inner.read(url).await
    }

    async fn write(&self, url: &str, data: &[u8]) -> Result<()> {
        self.inner.write(url, data).await
    }

    async fn delete(&self, url: &str) -> Result<()> {
        self.inner.delete(url).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        self.inner.delete_prefix(prefix).await
    }

    async fn exists(&self, url: &str) -> Result<bool> {
        self.inner.exists(url).await
    }

    fn register_with_datafusion(&self, ctx: &SessionContext) -> Result<()> {
        self.inner.register_with_datafusion(ctx)
    }

    fn prepare_cache_write(
        &self,
        connection_id: i32,
        schema: &str,
        table: &str,
    ) -> CacheWriteHandle {
        self.inner.prepare_cache_write(connection_id, schema, table)
    }

    async fn finalize_cache_write(&self, handle: &CacheWriteHandle) -> Result<String> {
        if self.fail_finalize.load(Ordering::SeqCst) {
            anyhow::bail!("Injected storage failure for testing")
        }
        self.inner.finalize_cache_write(handle).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_returns_result_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should have result_id (not null when persistence succeeds)
    assert!(json["result_id"].is_string());
    assert!(!json["result_id"].as_str().unwrap().is_empty());
    // Should not have warning
    assert!(json.get("warning").is_none());

    // Should have expected data
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_result_by_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Create a result
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 'hello' as greeting"}).to_string(),
                ))?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Fetch by ID
    let get_uri = PATH_RESULT.replace("{id}", result_id);
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;

    assert_eq!(get_json["result_id"], result_id);
    assert_eq!(get_json["rows"][0][0], "hello");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_nonexistent_result_returns_404() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let get_uri = PATH_RESULT.replace("{id}", "nonexistent-id");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_queries_get_unique_result_ids() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Execute first query
    let response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;

    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await?;
    let json1: serde_json::Value = serde_json::from_slice(&body1)?;
    let result_id1 = json1["result_id"].as_str().unwrap();

    // Execute second query
    let response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 2 as y"}).to_string()))?,
        )
        .await?;

    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await?;
    let json2: serde_json::Value = serde_json::from_slice(&body2)?;
    let result_id2 = json2["result_id"].as_str().unwrap();

    // Result IDs should be different
    assert_ne!(result_id1, result_id2);

    // Both should be retrievable
    let get_uri1 = PATH_RESULT.replace("{id}", result_id1);
    let get_response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri1)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_response1.status(), StatusCode::OK);

    let get_uri2 = PATH_RESULT.replace("{id}", result_id2);
    let get_response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri2)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_response2.status(), StatusCode::OK);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_result_returns_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    // Query that returns empty result
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT 1 as x WHERE false"}).to_string(),
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should still have result_id
    assert!(json["result_id"].is_string());
    let result_id = json["result_id"].as_str().unwrap();
    assert!(!result_id.is_empty());

    // But empty rows
    assert_eq!(json["row_count"], 0);

    // Verify we can retrieve the empty result by ID
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/results/{}", result_id))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;

    // Should have matching result_id
    assert_eq!(get_json["result_id"].as_str().unwrap(), result_id);
    // Should have the column name from the query
    assert_eq!(get_json["columns"].as_array().unwrap().len(), 1);
    assert_eq!(get_json["columns"][0], "x");
    // Should have empty rows
    assert_eq!(get_json["row_count"], 0);

    Ok(())
}

/// Test that storage failures result in null result_id with warning using injected failing storage.
#[tokio::test(flavor = "multi_thread")]
async fn test_persistence_failure_returns_null_result_id_with_warning() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Create a storage backend that we can make fail on demand
    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Query should still succeed, but with warning due to injected storage failure
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // result_id should be null (not missing)
    assert!(
        json.get("result_id").is_some(),
        "result_id field should be present"
    );
    assert!(
        json["result_id"].is_null(),
        "result_id should be null on persistence failure"
    );

    // Should have warning explaining the failure
    assert!(
        json.get("warning").is_some(),
        "warning field should be present"
    );
    assert!(
        json["warning"].as_str().unwrap().contains("not persisted"),
        "warning should explain persistence failure"
    );

    // Should still have the query results
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

/// Test that storage can recover after transient failures.
#[tokio::test(flavor = "multi_thread")]
async fn test_storage_recovery_after_failure() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Create a storage backend that starts in failing mode
    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage.clone())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // First query - storage is failing
    let response1 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;

    assert_eq!(response1.status(), StatusCode::OK);
    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX).await?;
    let json1: serde_json::Value = serde_json::from_slice(&body1)?;

    assert!(
        json1["result_id"].is_null(),
        "First query should have null result_id"
    );
    assert!(
        json1.get("warning").is_some(),
        "First query should have warning"
    );

    // Now fix the storage
    failing_storage.set_fail_finalize(false);

    // Second query - storage should work now
    let response2 = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 2 as y"}).to_string()))?,
        )
        .await?;

    assert_eq!(response2.status(), StatusCode::OK);
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await?;
    let json2: serde_json::Value = serde_json::from_slice(&body2)?;

    assert!(
        json2["result_id"].is_string(),
        "Second query should have valid result_id after storage recovery"
    );
    assert!(
        json2.get("warning").is_none(),
        "Second query should not have warning after storage recovery"
    );

    // Verify the result can be retrieved
    let result_id = json2["result_id"].as_str().unwrap();
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/results/{}", result_id))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);

    Ok(())
}

/// Test that multiple queries with storage failures all get proper warnings.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_queries_with_storage_failure() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Execute multiple queries, all should fail persistence but return results
    for i in 1..=3 {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_QUERY)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"sql": format!("SELECT {} as num", i)}).to_string(),
                    ))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        assert!(
            json["result_id"].is_null(),
            "Query {} should have null result_id",
            i
        );
        assert!(
            json.get("warning").is_some(),
            "Query {} should have warning",
            i
        );
        assert_eq!(
            json["rows"][0][0], i,
            "Query {} should return correct data",
            i
        );
    }

    Ok(())
}
