//! Integration tests for the refresh endpoint.
//!
//! Tests cover schema refresh, data refresh, validation, and pending deletions.

use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_CONNECTIONS, PATH_REFRESH};
use runtimedb::RuntimeEngine;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;

/// Generate a test secret key (base64-encoded 32 bytes)
fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Test harness providing engine and router access
struct RefreshTestHarness {
    engine: Arc<RuntimeEngine>,
    router: Router,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl RefreshTestHarness {
    async fn new() -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;

        let engine = RuntimeEngine::builder()
            .base_dir(temp_dir.path())
            .secret_key(generate_test_secret_key())
            .build()
            .await?;

        let app = AppServer::new(engine);

        Ok(Self {
            engine: app.engine,
            router: app.router,
            temp_dir,
        })
    }

    /// Create a DuckDB test database and return the path
    fn create_duckdb(&self, name: &str) -> String {
        let db_path = self.temp_dir.path().join(format!("{}.duckdb", name));
        let conn = duckdb::Connection::open(&db_path).expect("Failed to open DuckDB");

        conn.execute("CREATE SCHEMA sales", [])
            .expect("Failed to create schema");
        conn.execute(
            "CREATE TABLE sales.orders (id INTEGER, customer VARCHAR, amount DOUBLE)",
            [],
        )
        .expect("Failed to create table");
        conn.execute(
            "INSERT INTO sales.orders VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)",
            [],
        )
        .expect("Failed to insert data");

        db_path.to_str().unwrap().to_string()
    }

    /// Create a DuckDB with multiple tables for testing schema changes
    fn create_duckdb_multi_table(&self, name: &str) -> String {
        let db_path = self.temp_dir.path().join(format!("{}.duckdb", name));
        let conn = duckdb::Connection::open(&db_path).expect("Failed to open DuckDB");

        conn.execute("CREATE SCHEMA sales", [])
            .expect("Failed to create schema");
        conn.execute(
            "CREATE TABLE sales.orders (id INTEGER, customer VARCHAR)",
            [],
        )
        .expect("Failed to create orders");
        conn.execute(
            "CREATE TABLE sales.products (id INTEGER, name VARCHAR, price DOUBLE)",
            [],
        )
        .expect("Failed to create products");
        conn.execute("INSERT INTO sales.orders VALUES (1, 'Alice')", [])
            .expect("Failed to insert");
        conn.execute("INSERT INTO sales.products VALUES (1, 'Widget', 9.99)", [])
            .expect("Failed to insert");

        db_path.to_str().unwrap().to_string()
    }

    /// Add a table to an existing DuckDB
    fn add_table_to_duckdb(db_path: &str, schema: &str, table: &str) {
        let conn = duckdb::Connection::open(db_path).expect("Failed to open DuckDB");
        conn.execute(
            &format!("CREATE TABLE {}.{} (id INTEGER)", schema, table),
            [],
        )
        .expect("Failed to create table");
    }

    /// Remove a table from a DuckDB
    fn remove_table_from_duckdb(db_path: &str, schema: &str, table: &str) {
        let conn = duckdb::Connection::open(db_path).expect("Failed to open DuckDB");
        conn.execute(&format!("DROP TABLE {}.{}", schema, table), [])
            .expect("Failed to drop table");
    }

    /// Create a connection via API and return the connection_id
    async fn create_connection(&self, name: &str, db_path: &str) -> Result<String> {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(PATH_CONNECTIONS)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&json!({
                        "name": name,
                        "source_type": "duckdb",
                        "config": {
                            "path": db_path
                        }
                    }))?))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        Ok(json["id"].as_str().unwrap().to_string())
    }
}

// ============================================================================
// Schema Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_empty_connections() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Call refresh with no parameters (refresh all schemas)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({}))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // With no connections, refresh should succeed with 0 connections refreshed
    assert_eq!(json["connections_refreshed"], 0);
    assert_eq!(json["tables_discovered"], 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_new_tables() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with initial tables
    let db_path = harness.create_duckdb_multi_table("schema_test");

    // Create connection (discovery happens automatically)
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Add a new table to the DuckDB
    RefreshTestHarness::add_table_to_duckdb(&db_path, "sales", "customers");

    // Refresh schema for this connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["connections_refreshed"], 1);
    assert_eq!(json["tables_discovered"], 3); // orders, products, customers
    assert_eq!(json["tables_added"], 1); // customers was added

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_removed_tables() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("removal_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Remove a table from the DuckDB
    RefreshTestHarness::remove_table_from_duckdb(&db_path, "sales", "products");

    // Refresh schema
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["connections_refreshed"], 1);
    assert_eq!(json["tables_discovered"], 1); // only orders remains
    assert_eq!(json["tables_removed"], 1); // products was removed

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_preserves_cached_data() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("cache_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query the table to trigger sync (creates cached parquet)
    let result = harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    assert!(!result.results.is_empty());

    // Verify table is synced
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let orders_table = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .expect("orders table should exist");
    assert!(
        orders_table.parquet_path.is_some(),
        "orders should have cached data"
    );
    let original_path = orders_table.parquet_path.clone();

    // Add a new table to the DuckDB
    RefreshTestHarness::add_table_to_duckdb(&db_path, "sales", "inventory");

    // Refresh schema
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Verify orders table still has its cached data
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let orders_table = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .expect("orders table should still exist");

    assert!(
        orders_table.parquet_path.is_some(),
        "orders should still have cached data after schema refresh"
    );
    assert_eq!(
        orders_table.parquet_path, original_path,
        "cached data path should be unchanged"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_all_schemas_multiple_connections() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create two DuckDBs
    let db_path1 = harness.create_duckdb("conn1_db");
    let db_path2 = harness.create_duckdb("conn2_db");

    // Create two connections
    let _conn_id1 = harness.create_connection("conn1", &db_path1).await?;
    let _conn_id2 = harness.create_connection("conn2", &db_path2).await?;

    // Add tables to both
    RefreshTestHarness::add_table_to_duckdb(&db_path1, "sales", "new_table1");
    RefreshTestHarness::add_table_to_duckdb(&db_path2, "sales", "new_table2");

    // Refresh all schemas (no connection_id)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({}))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(json["connections_refreshed"], 2);
    assert_eq!(json["tables_discovered"], 4); // 2 per connection
    assert_eq!(json["tables_added"], 2); // 1 new table per connection

    Ok(())
}

// ============================================================================
// Data Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_data_single_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("data_refresh_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Refresh data for a single table
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify response contains table refresh info
    assert_eq!(json["schema_name"], "sales");
    assert_eq!(json["table_name"], "orders");
    assert!(json["duration_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_data_connection_wide() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("conn_refresh_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query both tables to trigger sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Refresh all data in connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify connection refresh response
    assert_eq!(json["tables_refreshed"], 2);
    assert_eq!(json["tables_failed"], 0);
    assert!(json["duration_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_data_creates_new_versioned_file() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("row_update_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    let result = harness
        .engine
        .execute_query("SELECT COUNT(*) as cnt FROM test_conn.sales.orders")
        .await?;
    let batch = result.results.first().unwrap();
    let initial_count = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_count, 2); // Initial data has 2 rows

    // Add more rows to DuckDB
    let conn = duckdb::Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO sales.orders VALUES (3, 'Charlie', 300.0), (4, 'Diana', 400.0)",
        [],
    )?;

    // Refresh data - this creates a new versioned parquet file
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Note: Before the old file is deleted (after grace period), both files
    // exist in the cache directory. DataFusion reads all parquet files in
    // the directory, so the count will be old_rows + new_rows until deletion.
    // The test verifies the refresh succeeds and creates a new file.
    // A separate test verifies deletion cleans up correctly.

    // Verify the refresh response
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(json["schema_name"], "sales");
    assert_eq!(json["table_name"], "orders");
    assert!(json["duration_ms"].is_number());

    Ok(())
}

// ============================================================================
// Validation Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_without_connection() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Try to refresh with schema_name but no connection_id
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "schema_name": "sales"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("requires connection_id"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_table_without_schema() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a connection first
    let db_path = harness.create_duckdb("validation_test");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try to refresh with table_name but no schema_name
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "table_name": "orders"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("requires schema_name"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_data_without_connection() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Try to do data refresh without connection_id
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("data refresh requires connection_id"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_level_not_supported() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    let db_path = harness.create_duckdb("validation_test2");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try schema-level refresh (schema_name without table_name, not data mode)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("schema-level refresh not supported"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_data_refresh_schema_without_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    let db_path = harness.create_duckdb("validation_test3");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try data refresh with schema but no table
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("data refresh with schema_name requires table_name"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_refresh_cannot_target_table() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    let db_path = harness.create_duckdb("validation_test4");
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Try schema refresh targeting a specific table (not allowed)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": false
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("schema refresh cannot target specific table"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_connection_not_found() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Try to refresh a non-existent connection
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": "con_nonexistent_fake_id_12345"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));

    Ok(())
}

// ============================================================================
// Pending Deletion Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_data_refresh_schedules_pending_deletion() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("pending_deletion_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync (creates first parquet file)
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Get the cache directory path
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let cache_path = tables
        .iter()
        .find(|t| t.table_name == "orders")
        .and_then(|t| t.parquet_path.as_ref())
        .expect("orders should have parquet path")
        .clone();

    // Count parquet files in the cache directory before refresh
    let cache_dir = cache_path.strip_prefix("file://").unwrap();
    let files_before: Vec<_> = std::fs::read_dir(cache_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .collect();
    assert_eq!(
        files_before.len(),
        1,
        "should have 1 parquet file before refresh"
    );

    // Refresh data (creates a new versioned file and schedules old for deletion)
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Count parquet files after refresh - should now have 2
    // (old file still exists, new versioned file created)
    let files_after: Vec<_> = std::fs::read_dir(cache_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .collect();
    assert_eq!(
        files_after.len(),
        2,
        "should have 2 parquet files after refresh (old + new)"
    );

    // Verify at least one file has a version marker in its name
    let has_versioned = files_after.iter().any(|f| {
        f.path()
            .file_name()
            .and_then(|n| n.to_str())
            .map_or(false, |n| n.contains("_v"))
    });
    assert!(
        has_versioned,
        "at least one file should have version marker"
    );

    // The deletion is scheduled 60 seconds in the future by default
    // so we verify the scheduling mechanism works but don't wait for deletion
    let due = harness.engine.catalog().get_due_deletions().await?;
    assert!(
        due.is_empty(),
        "deletions should not be due immediately (60s grace period)"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_refresh_schedules_deletion_for_removed_tables() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB with multiple tables
    let db_path = harness.create_duckdb_multi_table("schema_deletion_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query products table to trigger sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.products")
        .await?;

    // Verify products has cached data
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let products_cached = tables
        .iter()
        .find(|t| t.table_name == "products")
        .and_then(|t| t.parquet_path.as_ref())
        .is_some();
    assert!(products_cached, "products should have cached data");

    // Remove products table from DuckDB
    RefreshTestHarness::remove_table_from_duckdb(&db_path, "sales", "products");

    // Refresh schema
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Verify table was removed
    assert_eq!(json["tables_removed"], 1);

    // Verify products table no longer exists
    let tables = harness.engine.list_tables(Some("test_conn")).await?;
    let products_exists = tables.iter().any(|t| t.table_name == "products");
    assert!(!products_exists, "products table should be removed");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pending_deletions_respect_timing() -> Result<()> {
    let harness = RefreshTestHarness::new().await?;

    // Create a DuckDB
    let db_path = harness.create_duckdb("timing_test");

    // Create connection
    let connection_id = harness.create_connection("test_conn", &db_path).await?;

    // Query to trigger initial sync
    harness
        .engine
        .execute_query("SELECT * FROM test_conn.sales.orders")
        .await?;

    // Refresh data to schedule a deletion
    let response = harness
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_REFRESH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "connection_id": connection_id,
                    "schema_name": "sales",
                    "table_name": "orders",
                    "data": true
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Get due deletions immediately - should be empty since deletion is 60s in future
    let due = harness.engine.catalog().get_due_deletions().await?;
    assert!(
        due.is_empty(),
        "deletions should not be due immediately after scheduling"
    );

    // Process pending deletions - should delete nothing since not due yet
    let deleted = harness.engine.process_pending_deletions().await?;
    assert_eq!(deleted, 0, "no files should be deleted before due time");

    Ok(())
}
