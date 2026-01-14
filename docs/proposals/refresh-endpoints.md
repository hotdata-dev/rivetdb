# Refresh Endpoints Proposal

## Overview

This proposal introduces three refresh endpoints to runtimedb:

1. **Refresh Table** - Reload data for a specific table from the remote source
2. **Refresh Connection** - Reload all table data for a connection
3. **Refresh Information Schema** - Re-discover table/column metadata without reloading data

All data refresh operations are designed to be **non-blocking**: queries continue to use existing cached data until new data is fully available, at which point an atomic swap occurs.

## API Design

### 1. Refresh Table

```
POST /connections/{name}/tables/{schema}/{table}/refresh
```

**Behavior:**
- Fetches fresh data from the remote source into a new cache location
- Existing queries continue using the old cached data during fetch
- Once complete, atomically updates catalog to point to new data
- Deletes old cache files after successful swap

**Response (202 Accepted):**
```json
{
  "refresh_id": "uuid",
  "connection": "my_postgres",
  "schema": "public",
  "table": "users",
  "status": "in_progress",
  "started_at": "2026-01-14T10:30:00Z"
}
```

**Response (200 OK - synchronous mode):**
```json
{
  "connection": "my_postgres",
  "schema": "public",
  "table": "users",
  "status": "completed",
  "started_at": "2026-01-14T10:30:00Z",
  "completed_at": "2026-01-14T10:30:05Z",
  "rows_synced": 15000
}
```

**Query Parameters:**
- `async=true|false` (default: `false`) - Run refresh in background
- `force=true|false` (default: `false`) - Refresh even if recently synced

### 2. Refresh Connection

```
POST /connections/{name}/refresh
```

**Behavior:**
- Refreshes all tables in the connection in parallel (with concurrency limit)
- Each table follows the same atomic swap pattern
- Reports progress and completion status

**Request Body (optional):**
```json
{
  "concurrency": 5,
  "tables": ["public.users", "public.orders"]  // Optional: specific tables only
}
```

**Response (202 Accepted):**
```json
{
  "refresh_id": "uuid",
  "connection": "my_postgres",
  "status": "in_progress",
  "tables_total": 25,
  "tables_queued": 25,
  "started_at": "2026-01-14T10:30:00Z"
}
```

### 3. Refresh Information Schema

```
POST /connections/{name}/discover
```

> Note: This endpoint already exists but could be enhanced.

**Current Behavior:**
- Re-introspects remote database for tables/columns
- Updates catalog with new/changed tables
- Does NOT refresh cached data

**Enhanced Behavior (proposed):**
- Add query parameter `?invalidate_changed=true` to clear cache for tables whose schema changed
- Detect schema drift and report changed tables

**Response:**
```json
{
  "name": "my_postgres",
  "tables_discovered": 30,
  "tables_added": 2,
  "tables_removed": 1,
  "tables_schema_changed": 3,
  "discovery_status": "success"
}
```

### 4. Refresh Status (for async operations)

```
GET /refresh/{refresh_id}
```

**Response:**
```json
{
  "refresh_id": "uuid",
  "connection": "my_postgres",
  "status": "in_progress",
  "tables_total": 25,
  "tables_completed": 12,
  "tables_failed": 1,
  "started_at": "2026-01-14T10:30:00Z",
  "errors": [
    {
      "table": "public.large_table",
      "error": "Connection timeout"
    }
  ]
}
```

---

## Architecture

### Atomic Refresh Pattern

The key design principle is **atomic swap** - queries never see partial data:

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Refresh request received                                     │
├─────────────────────────────────────────────────────────────────┤
│ 2. Generate new cache path with unique suffix                   │
│    Old: /cache/conn_1/public/users/data.parquet                 │
│    New: /cache/conn_1/public/users/data_v2_abc123.parquet       │
├─────────────────────────────────────────────────────────────────┤
│ 3. Fetch data from remote → write to NEW path                   │
│    (Existing queries continue using old path)                   │
├─────────────────────────────────────────────────────────────────┤
│ 4. On success: Atomic catalog update                            │
│    UPDATE tables SET parquet_path = $new_path,                  │
│                      last_sync = NOW()                          │
│    WHERE id = $table_id                                         │
├─────────────────────────────────────────────────────────────────┤
│ 5. Delete old cache file (with grace period)                    │
│    - Option A: Delete after short delay (e.g., 30s)             │
│    - Option B: Background cleanup of old versions               │
└─────────────────────────────────────────────────────────────────┘
```

**Why no catalog provider re-registration is needed:**

`LazyTableProvider.scan()` fetches `parquet_path` from the catalog on every query execution (not cached in the provider). Once the catalog is updated, new queries automatically use the new path. In-flight queries that already resolved the old path will complete using the old file.

The only concern is deleting old files while queries are still using them. Solutions:
- **Grace period**: Wait 30-60 seconds before deleting old files
- **Reference counting**: Track active scans per file (more complex)
- **Lazy cleanup**: Mark old files for deletion, clean up periodically

### Engine Methods

```rust
impl RuntimeEngine {
    /// Refresh a single table's cached data atomically.
    pub async fn refresh_table(
        &self,
        connection_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<RefreshResult>;

    /// Refresh all tables in a connection with concurrency control.
    pub async fn refresh_connection(
        &self,
        name: &str,
        options: RefreshConnectionOptions,
    ) -> Result<RefreshConnectionResult>;

    /// Spawn background refresh task, returns immediately.
    pub fn spawn_refresh_table(
        &self,
        connection_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> RefreshHandle;

    /// Spawn background connection refresh, returns immediately.
    pub fn spawn_refresh_connection(
        &self,
        name: &str,
        options: RefreshConnectionOptions,
    ) -> RefreshHandle;
}
```

### New Types

```rust
/// Options for connection refresh
pub struct RefreshConnectionOptions {
    /// Max concurrent table refreshes (default: 5)
    pub concurrency: usize,
    /// Specific tables to refresh (None = all tables)
    pub tables: Option<Vec<String>>,
}

/// Result of a table refresh operation
pub struct RefreshResult {
    pub connection: String,
    pub schema: String,
    pub table: String,
    pub rows_synced: usize,
    pub duration_ms: u64,
    pub old_path: Option<String>,
    pub new_path: String,
}

/// Result of a connection refresh operation
pub struct RefreshConnectionResult {
    pub connection: String,
    pub tables_refreshed: usize,
    pub tables_failed: usize,
    pub total_rows: usize,
    pub duration_ms: u64,
    pub errors: Vec<TableRefreshError>,
}

/// Handle to a background refresh task
pub struct RefreshHandle {
    pub refresh_id: Uuid,
    pub status: Arc<RwLock<RefreshStatus>>,
    pub join_handle: JoinHandle<Result<()>>,
}

#[derive(Clone)]
pub enum RefreshStatus {
    Pending,
    InProgress {
        tables_completed: usize,
        tables_total: usize
    },
    Completed(RefreshConnectionResult),
    Failed(String),
}
```

### FetchOrchestrator Enhancement

```rust
impl FetchOrchestrator {
    /// Refresh table with atomic swap semantics.
    /// Returns (new_path, old_path) on success.
    pub async fn refresh_table(
        &self,
        source: &Source,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(String, Option<String>)> {
        // 1. Get current path (will be deleted after grace period)
        let old_info = self.catalog
            .get_table(connection_id, schema_name, table_name)
            .await?;
        let old_path = old_info.and_then(|i| i.parquet_path);

        // 2. Generate unique new path (append timestamp/uuid)
        let new_path = self.storage
            .prepare_versioned_cache_write(connection_id, schema_name, table_name);

        // 3. Fetch and write to new path
        let mut writer = StreamingParquetWriter::new(new_path.clone());
        self.fetcher.fetch_table(source, &self.secret_manager,
                                  None, schema_name, table_name, &mut writer).await?;
        writer.close()?;

        // 4. Finalize (upload to S3 if needed)
        let parquet_url = self.storage
            .finalize_cache_write(&new_path, connection_id, schema_name, table_name)
            .await?;

        // 5. Atomic catalog update - new queries immediately use new path
        if let Some(info) = self.catalog
            .get_table(connection_id, schema_name, table_name)
            .await?
        {
            self.catalog.update_table_sync(info.id, &parquet_url).await?;
        }

        // 6. Return old_path for deferred cleanup (caller schedules deletion)
        Ok((parquet_url, old_path))
    }
}
```

### Storage Enhancement

```rust
impl StorageManager {
    /// Prepare a versioned cache write path (for atomic refresh).
    fn prepare_versioned_cache_write(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> PathBuf {
        let version = Uuid::new_v4().to_string()[..8].to_string();
        let base = self.cache_dir
            .join(connection_id.to_string())
            .join(schema_name)
            .join(table_name);
        base.join(format!("data_{}.parquet", version))
    }
}
```

---

## Background Task Management

For async refreshes, we need a simple task registry:

```rust
pub struct RefreshRegistry {
    tasks: Arc<RwLock<HashMap<Uuid, RefreshHandle>>>,
}

impl RefreshRegistry {
    pub fn register(&self, handle: RefreshHandle) -> Uuid;
    pub fn get_status(&self, id: Uuid) -> Option<RefreshStatus>;
    pub fn cleanup_completed(&self); // Remove old completed tasks
}
```

This would be added to `RuntimeEngine` and cleaned up periodically.

---

## HTTP Routes

Add to `app_server.rs`:

```rust
.route(
    "/connections/:name/refresh",
    post(refresh_connection_handler)
)
.route(
    "/connections/:name/tables/:schema/:table/refresh",
    post(refresh_table_handler)
)
.route(
    "/refresh/:refresh_id",
    get(get_refresh_status_handler)
)
```

---

## Handler Implementation Sketch

```rust
/// Handler for POST /connections/{name}/tables/{schema}/{table}/refresh
pub async fn refresh_table_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(params): Path<TableRefreshPath>,
    Query(opts): Query<RefreshOptions>,
) -> Result<Json<RefreshTableResponse>, ApiError> {
    // Validate connection and table exist
    let conn = engine.catalog().get_connection(&params.name).await?
        .ok_or_else(|| ApiError::not_found("Connection not found"))?;

    let table = engine.catalog()
        .get_table(conn.id, &params.schema, &params.table).await?
        .ok_or_else(|| ApiError::not_found("Table not found"))?;

    if opts.async_mode {
        // Spawn background task
        let handle = engine.spawn_refresh_table(
            &params.name, &params.schema, &params.table
        );

        Ok(Json(RefreshTableResponse {
            refresh_id: Some(handle.refresh_id),
            status: "in_progress".into(),
            ..
        }))
    } else {
        // Synchronous refresh
        let result = engine.refresh_table(
            &params.name, &params.schema, &params.table
        ).await?;

        Ok(Json(RefreshTableResponse {
            status: "completed".into(),
            rows_synced: Some(result.rows_synced),
            ..
        }))
    }
}
```

---

## Implementation Phases

### Phase 1: Synchronous Table Refresh
1. Add `refresh_table()` method to `RuntimeEngine`
2. Implement versioned cache paths in `StorageManager`
3. Add `POST /connections/{name}/tables/{schema}/{table}/refresh` endpoint
4. Add old file cleanup logic after atomic swap

### Phase 2: Synchronous Connection Refresh
1. Add `refresh_connection()` method with parallel execution
2. Add `POST /connections/{name}/refresh` endpoint
3. Implement concurrency limiting with semaphore

### Phase 3: Enhanced Discovery
1. Enhance `discover_connection()` to detect schema changes
2. Add `invalidate_changed` option to clear stale caches
3. Return detailed discovery diff in response

### Phase 4: Async Operations (if needed)
1. Add `RefreshRegistry` for background task tracking
2. Add `spawn_refresh_*` methods
3. Add `GET /refresh/{id}` status endpoint
4. Add periodic cleanup of completed tasks

---

## Considerations

### Concurrency Safety
- The catalog database provides atomicity for metadata updates
- `LazyTableProvider` reads `parquet_path` fresh on each `scan()`, so no provider re-registration needed
- Old files should be deleted after a grace period to allow in-flight queries to complete

### Error Handling
- If fetch fails, old cache remains valid (no data loss)
- Partial connection refresh should continue with other tables
- Failed tables should be reported but not block others

### Resource Management
- Large tables may require significant memory during fetch
- Consider streaming writes (already implemented in `StreamingParquetWriter`)
- Concurrency limit prevents overwhelming remote sources

### S3 Considerations
- Versioned paths work naturally with S3
- Old objects can be deleted after swap
- Consider S3 lifecycle rules for cleanup of orphaned files

---

## Alternatives Considered

### Alternative 1: In-place Refresh
Write new data to the same path, overwriting old data.

**Rejected because:** Queries in progress would see partial/corrupt data.

### Alternative 2: Double-buffering with Symlinks
Maintain two data directories and swap symlinks.

**Rejected because:** Adds complexity; versioned paths achieve same goal more simply.

### Alternative 3: External Job Queue (Redis, etc.)
Use external queue for background tasks.

**Rejected because:** Adds operational complexity; in-process tasks sufficient for single-node architecture.
