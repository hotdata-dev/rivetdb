# RuntimeDB Test Coverage Review

A comprehensive analysis of the test suite identifying low-value tests and missing coverage.

## Executive Summary

The codebase has **~4,600 lines of integration tests** and **~60+ test functions**, but there are significant gaps in test coverage, particularly around:
- Core orchestration components (FetchOrchestrator, LazyTableProvider)
- Configuration loading and validation
- Error recovery and edge cases
- Concurrency scenarios

---

## Part 1: Low-Value Tests

### 1.1 Tests for Unimplemented Features

**File:** `tests/engine_sync_tests.rs`

Both tests are `#[ignore]` and test a function that isn't implemented:

```rust
// Line 476 in src/engine.rs
pub async fn sync_connection(&self, _name: &str) -> Result<()> {
    todo!("Implement connections sync")
}
```

**Tests affected:**
- `test_sync_connection_not_found` (line 17)
- `test_sync_connection_no_tables` (line 37)

**Recommendation:** Remove these tests until the feature is implemented, or implement the feature.

---

### 1.2 Tests Requiring External Infrastructure (Not Run by Default)

**File:** `tests/s3_storage_tests.rs`

Three tests are marked `#[ignore]` requiring external MinIO setup:
- `s3_storage_write_read_delete` (line 14)
- `s3_storage_delete_prefix` (line 44)
- `s3_storage_path_construction` (line 76)

**Impact:** S3 storage is untested in regular CI runs.

**Recommendation:** Use testcontainers for MinIO (like the Iceberg tests do) to make these tests run automatically.

---

### 1.3 Misleading Test Names

**File:** `tests/datafetch_tests.rs:92-110`

```rust
#[tokio::test]
async fn test_unsupported_driver() {
    // Uses a Snowflake source with no credentials - should fail with connection error
    let source = Source::Snowflake { ... };
    let result = fetcher.discover_tables(&source, &secrets).await;
    assert!(result.is_err(), "Should fail without valid credentials");
}
```

**Problem:** The test is named "unsupported_driver" but actually tests credential validation failure for Snowflake, which IS a supported driver.

**Recommendation:** Rename to `test_snowflake_discovery_fails_without_credentials`.

---

### 1.4 Tests That Test Dependencies, Not Implementation

**File:** `tests/http_server_tests.rs`

Several tests are essentially DataFusion SQL capability tests rather than RuntimeDB HTTP layer tests:

| Test | Lines | What It Actually Tests |
|------|-------|----------------------|
| `test_query_endpoint_aggregate_functions` | 279-297 | DataFusion COUNT/SUM/AVG |
| `test_query_endpoint_group_by` | 299-321 | DataFusion GROUP BY |
| `test_query_endpoint_joins` | 323-345 | DataFusion JOIN |

**Problem:** These tests add execution time but don't test RuntimeDB-specific behavior. If DataFusion's SQL engine breaks, these tests would fail, but DataFusion has its own extensive test suite.

**Recommendation:** Keep one or two basic SQL tests to verify query endpoint works, remove the comprehensive SQL feature tests.

---

### 1.5 Pure Serialization Tests

**File:** `src/source.rs` (tests module, lines 150-552)

Contains 20+ tests that only verify serde serialization/deserialization:

```rust
#[test]
fn test_postgres_serialization() {
    let source = Source::Postgres { ... };
    let json = serde_json::to_string(&source).unwrap();
    assert!(json.contains(r#""type":"postgres""#));
    let parsed: Source = serde_json::from_str(&json).unwrap();
    assert_eq!(source, parsed);
}
```

**Problem:** These test that serde works correctly, not that the application logic is correct. If serde has bugs, serde's tests would catch them.

**Recommendation:** Keep 1-2 representative serialization tests as smoke tests; remove the exhaustive coverage for each variant.

---

## Part 2: Critical Missing Test Coverage

### 2.1 FetchOrchestrator - ZERO Direct Tests

**File:** `src/datafetch/orchestrator.rs`

This 89-line module orchestrates the core data fetching workflow:
1. Prepare cache write location
2. Create parquet writer
3. Fetch table data
4. Close writer
5. Finalize cache write
6. Update catalog

**Current status:** Only tested indirectly through integration tests.

**Missing tests:**
- What happens when storage.prepare_cache_write fails?
- What happens when fetcher.fetch_table fails mid-stream?
- What happens when writer.close() fails?
- What happens when storage.finalize_cache_write fails?
- What happens when catalog.update_table_sync fails after successful write?

**Risk:** High - this is a critical data path.

---

### 2.2 LazyTableProvider - ZERO Direct Tests

**File:** `src/datafusion/lazy_table_provider.rs` (164 lines)

This component:
- Provides table schema to DataFusion
- Lazily fetches data on first scan
- Handles projection, filter, and limit pushdown
- Converts parquet URLs to ListingTable

**Missing tests:**
- `schema()` returns correct schema without I/O
- `scan()` triggers fetch when not cached
- `scan()` uses cached path when available
- `supports_filters_pushdown()` behavior
- Error handling when catalog.get_table fails
- Error handling when fetch_and_cache fails
- Handling of different URL schemes (file://, s3://)

**Risk:** High - this is the core DataFusion integration point.

---

### 2.3 Configuration Module - ZERO Tests

**File:** `src/config/mod.rs` (131 lines)

**Missing tests:**
- `AppConfig::load()` from file
- Environment variable overrides (RUNTIMEDB_*)
- `AppConfig::validate()` for all catalog types
- `AppConfig::validate()` for all storage types
- Invalid configuration rejection
- Default value application

**Risk:** Medium - config errors could cause production issues.

---

### 2.4 RuntimeCatalogProvider / RuntimeSchemaProvider - ZERO Direct Tests

**Files:**
- `src/datafusion/catalog_provider.rs`
- `src/datafusion/schema_provider.rs`

These implement DataFusion's `CatalogProvider` and `SchemaProvider` traits.

**Missing tests:**
- Schema listing
- Table resolution
- Case sensitivity handling
- Non-existent schema/table handling

---

### 2.5 Snowflake Data Fetcher - No Integration Tests

**File:** `src/datafetch/native/snowflake.rs`

**Current status:** Only tested for credential failure (misnamed as "unsupported_driver").

**Missing tests:**
- Successful table discovery
- Successful data fetch
- Schema mapping from Snowflake types to Arrow
- Warehouse/role handling
- Error handling for API failures

**Risk:** Medium - Snowflake is a supported production source.

---

### 2.6 Motherduck Data Fetcher - No Integration Tests

**File:** `src/datafetch/native/duckdb.rs` (handles Motherduck via token auth)

**Missing tests:**
- Motherduck token authentication
- Database catalog filtering
- Cloud connection handling

---

### 2.7 Iceberg Glue Catalog - No Integration Tests

**File:** `src/datafetch/native/iceberg.rs`

**Current status:** REST catalog is tested; Glue catalog is not.

**Missing tests:**
- Glue catalog connection
- AWS credential handling for Glue
- Glue-specific namespace handling

---

## Part 3: Missing Edge Case Coverage

### 3.1 Concurrency

| Scenario | Current Coverage |
|----------|-----------------|
| Concurrent queries to same table | None |
| Concurrent connection creation with same name | None |
| Concurrent cache purge operations | None |
| Concurrent secret access | None |

### 3.2 Error Recovery

| Scenario | Current Coverage |
|----------|-----------------|
| Partial sync failure recovery | None |
| Corrupted parquet file handling | None |
| Database connection loss during fetch | None |
| Storage write failure mid-operation | None |
| Catalog update failure after parquet write | None |

### 3.3 Data Scale

| Scenario | Current Coverage |
|----------|-----------------|
| Tables with millions of rows | None |
| Memory pressure during large fetches | None |
| Parquet file size limits | None |
| Query result size limits | None |

### 3.4 Schema Evolution

| Scenario | Current Coverage |
|----------|-----------------|
| Remote table schema changes after sync | None |
| Column addition/removal handling | None |
| Type change handling | None |

### 3.5 Connection Lifecycle

| Scenario | Current Coverage |
|----------|-----------------|
| Connection re-registration after delete | None |
| Stale connection detection | None |
| Connection timeout handling | None |
| Graceful shutdown during query | None |

### 3.6 HTTP API Edge Cases

| Scenario | Current Coverage |
|----------|-----------------|
| Request timeouts | None |
| Large query results (pagination) | None |
| Unicode in connection names | None |
| Special characters in SQL queries | Basic |

### 3.7 Storage Edge Cases

| Scenario | Current Coverage |
|----------|-----------------|
| S3 bucket permission errors | None |
| S3 network timeouts | None |
| S3 eventual consistency | None |
| Filesystem permission errors | None |
| Disk full scenarios | None |

---

## Part 4: Recommendations

### Immediate Actions (High Priority)

1. **Add FetchOrchestrator unit tests** - This is the core data path
2. **Add LazyTableProvider unit tests** - Critical DataFusion integration
3. **Fix S3 storage tests to use testcontainers** - Enable automated S3 testing
4. **Add Config module tests** - Prevent production config issues

### Medium Priority

5. **Remove or rename misleading tests** - `test_unsupported_driver`
6. **Reduce DataFusion SQL feature tests** - Keep 1-2, remove rest
7. **Add concurrency tests** - At least for connection creation
8. **Add error recovery tests** - Especially for FetchOrchestrator

### Low Priority

9. **Reduce serialization test coverage** - Keep representative samples
10. **Add Snowflake integration tests** (requires infrastructure)
11. **Add schema evolution tests**
12. **Add scale tests** (requires infrastructure)

---

## Appendix: Test File Summary

| File | Lines | Tests | Quality Assessment |
|------|-------|-------|-------------------|
| `integration_tests.rs` | 1,207 | 26 | Good - unified test harness |
| `http_server_tests.rs` | 1,270 | ~40 | Mixed - some test DataFusion |
| `catalog_manager_suite.rs` | 614 | 19×2 | Good - macro-based dual-backend |
| `iceberg_tests.rs` | 539 | 3 | Good - real infrastructure |
| `datafetch_tests.rs` | 378 | 6 | Fair - missing Snowflake/Motherduck |
| `information_schema_tests.rs` | 173 | 4 | Good |
| `storage_tests.rs` | 170 | 8 | Good for filesystem |
| `s3_storage_tests.rs` | 136 | 5 | Poor - 3 ignored |
| `engine_sync_tests.rs` | 73 | 2 | Poor - both ignored |
| `src/secrets/mod.rs` (inline) | ~230 | 14 | Good |
| `src/secrets/encryption.rs` (inline) | ~100 | 10 | Good |
| `src/secrets/encrypted_catalog_backend.rs` (inline) | ~175 | 9 | Good |
| `src/source.rs` (inline) | ~400 | 20+ | Excessive - mostly serialization |
| `src/http/serialization.rs` (inline) | ~150 | 12 | Good |
| `src/engine.rs` (inline) | ~100 | 4 | Fair |
