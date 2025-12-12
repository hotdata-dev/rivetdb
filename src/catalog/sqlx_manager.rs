use super::TableInfo;
use crate::catalog::manager::{CatalogManager, ConnectionInfo};
use anyhow::Result;
use sqlx::{Database, Executor, Pool, Row};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use tokio::task::block_in_place;

/// Trait abstracting database-specific behavior for sqlx-based catalog managers.
///
/// All SQL queries have default implementations using `?` placeholders (SQLite style).
/// Override methods where syntax differs (Postgres uses $1/$2, different DDL, etc.).
pub trait SqlxBackend: Send + Sync + 'static {
    type DB: Database;

    /// Format a connection string for this database type.
    fn connection_uri(path: &str) -> String;

    // ========================================================================
    // Schema creation
    // ========================================================================

    fn create_migrations_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    }

    fn create_connections_table_sql() -> &'static str;
    fn create_tables_table_sql() -> &'static str;

    // ========================================================================
    // Migrations
    // ========================================================================

    fn select_max_migration_version_sql() -> &'static str {
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
    }

    fn migration_1_sql() -> &'static str;

    fn migration_2_sql() -> &'static str {
        "ALTER TABLE tables ADD COLUMN arrow_schema_json TEXT"
    }

    fn insert_migration_version_sql() -> &'static str {
        "INSERT INTO schema_migrations (version) VALUES (?)"
    }

    // ========================================================================
    // Connections queries
    // ========================================================================

    fn list_connections_sql() -> &'static str {
        "SELECT id, name, source_type, config_json FROM connections ORDER BY name"
    }

    fn insert_connection_sql() -> &'static str {
        "INSERT INTO connections (name, source_type, config_json) VALUES (?, ?, ?)"
    }

    fn select_connection_id_by_name_sql() -> &'static str {
        "SELECT id FROM connections WHERE name = ?"
    }

    fn select_connection_by_name_sql() -> &'static str {
        "SELECT id, name, source_type, config_json FROM connections WHERE name = ?"
    }

    fn delete_connection_by_id_sql() -> &'static str {
        "DELETE FROM connections WHERE id = ?"
    }

    // ========================================================================
    // Tables queries
    // ========================================================================

    fn upsert_table_sql() -> &'static str {
        "INSERT INTO tables (connection_id, schema_name, table_name, arrow_schema_json)
         VALUES (?, ?, ?, ?)
         ON CONFLICT (connection_id, schema_name, table_name)
         DO UPDATE SET arrow_schema_json = excluded.arrow_schema_json"
    }

    fn select_table_id_by_key_sql() -> &'static str {
        "SELECT id FROM tables WHERE connection_id = ? AND schema_name = ? AND table_name = ?"
    }

    fn select_table_by_key_sql() -> &'static str {
        "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path,
                last_sync, arrow_schema_json
         FROM tables WHERE connection_id = ? AND schema_name = ? AND table_name = ?"
    }

    fn list_tables_all_sql() -> &'static str {
        "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path,
                last_sync, arrow_schema_json
         FROM tables ORDER BY schema_name, table_name"
    }

    fn list_tables_by_connection_sql() -> &'static str {
        "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path,
                last_sync, arrow_schema_json
         FROM tables WHERE connection_id = ? ORDER BY schema_name, table_name"
    }

    fn update_table_sync_sql() -> &'static str {
        "UPDATE tables SET parquet_path = ?, state_path = ?, last_sync = CURRENT_TIMESTAMP WHERE id = ?"
    }

    fn clear_table_cache_sql() -> &'static str {
        "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE id = ?"
    }

    fn clear_connection_cache_sql() -> &'static str {
        "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE connection_id = ?"
    }

    fn delete_tables_by_connection_sql() -> &'static str {
        "DELETE FROM tables WHERE connection_id = ?"
    }
}

// ============================================================================
// PostgreSQL Backend
// ============================================================================

pub struct PostgresBackend;

impl SqlxBackend for PostgresBackend {
    type DB = sqlx::Postgres;

    fn connection_uri(connection_string: &str) -> String {
        connection_string.to_string()
    }

    fn create_connections_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS connections (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            source_type TEXT NOT NULL,
            config_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    }

    fn create_tables_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS tables (
            id SERIAL PRIMARY KEY,
            connection_id INTEGER NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            parquet_path TEXT,
            state_path TEXT,
            last_sync TIMESTAMP,
            arrow_schema_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (connection_id) REFERENCES connections(id),
            UNIQUE (connection_id, schema_name, table_name)
        )"
    }

    fn migration_1_sql() -> &'static str {
        "UPDATE connections
         SET config_json = config_json::jsonb || '{\"type\":\"postgres\"}'::jsonb
         WHERE config_json::jsonb->>'type' IS NULL"
    }

    fn migration_2_sql() -> &'static str {
        "ALTER TABLE tables ADD COLUMN IF NOT EXISTS arrow_schema_json TEXT"
    }

    // Postgres uses $1, $2, etc. for placeholders
    fn insert_migration_version_sql() -> &'static str {
        "INSERT INTO schema_migrations (version) VALUES ($1)"
    }

    fn insert_connection_sql() -> &'static str {
        "INSERT INTO connections (name, source_type, config_json) VALUES ($1, $2, $3)"
    }

    fn select_connection_id_by_name_sql() -> &'static str {
        "SELECT id FROM connections WHERE name = $1"
    }

    fn select_connection_by_name_sql() -> &'static str {
        "SELECT id, name, source_type, config_json FROM connections WHERE name = $1"
    }

    fn delete_connection_by_id_sql() -> &'static str {
        "DELETE FROM connections WHERE id = $1"
    }

    fn upsert_table_sql() -> &'static str {
        "INSERT INTO tables (connection_id, schema_name, table_name, arrow_schema_json)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (connection_id, schema_name, table_name)
         DO UPDATE SET arrow_schema_json = excluded.arrow_schema_json"
    }

    fn select_table_id_by_key_sql() -> &'static str {
        "SELECT id FROM tables WHERE connection_id = $1 AND schema_name = $2 AND table_name = $3"
    }

    fn select_table_by_key_sql() -> &'static str {
        "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path,
                CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json
         FROM tables WHERE connection_id = $1 AND schema_name = $2 AND table_name = $3"
    }

    fn list_tables_all_sql() -> &'static str {
        "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path,
                CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json
         FROM tables ORDER BY schema_name, table_name"
    }

    fn list_tables_by_connection_sql() -> &'static str {
        "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path,
                CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json
         FROM tables WHERE connection_id = $1 ORDER BY schema_name, table_name"
    }

    fn update_table_sync_sql() -> &'static str {
        "UPDATE tables SET parquet_path = $1, state_path = $2, last_sync = CURRENT_TIMESTAMP WHERE id = $3"
    }

    fn clear_table_cache_sql() -> &'static str {
        "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE id = $1"
    }

    fn clear_connection_cache_sql() -> &'static str {
        "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE connection_id = $1"
    }

    fn delete_tables_by_connection_sql() -> &'static str {
        "DELETE FROM tables WHERE connection_id = $1"
    }
}

// ============================================================================
// SQLite Backend
// ============================================================================

pub struct SqliteBackend;

impl SqlxBackend for SqliteBackend {
    type DB = sqlx::Sqlite;

    fn connection_uri(path: &str) -> String {
        format!("sqlite:{}?mode=rwc", path)
    }

    fn create_connections_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            source_type TEXT NOT NULL,
            config_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    }

    fn create_tables_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_id INTEGER NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            parquet_path TEXT,
            state_path TEXT,
            last_sync TIMESTAMP,
            arrow_schema_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (connection_id) REFERENCES connections(id),
            UNIQUE (connection_id, schema_name, table_name)
        )"
    }

    fn migration_1_sql() -> &'static str {
        "UPDATE connections
         SET config_json = json_patch(config_json, '{\"type\":\"postgres\"}')
         WHERE json_extract(config_json, '$.type') IS NULL"
    }

    // SQLite uses default `?` placeholders - no overrides needed for queries
}

// ============================================================================
// Generic SqlxCatalogManager
// ============================================================================

pub struct SqlxCatalogManager<B: SqlxBackend>
where
        for<'c> &'c Pool<B::DB>: Executor<'c, Database = B::DB>,
{
    pool: Pool<B::DB>,
    catalog_path: String,
}

impl<B: SqlxBackend> SqlxCatalogManager<B>
where
    for<'c> &'c Pool<B::DB>: Executor<'c, Database = B::DB>,
    for<'r> i32: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> i64: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> String: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> Option<String>: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> &'r str: sqlx::Encode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> i32: sqlx::Encode<'r, B::DB>,
    <B::DB as Database>::Row: Row,
    usize: sqlx::ColumnIndex<<B::DB as Database>::Row>,
{
    pub fn new(path: &str) -> Result<Self> {
        let pool = block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let uri = B::connection_uri(path);
                let pool = Pool::<B::DB>::connect(&uri).await?;
                Self::initialize_schema_async(&pool).await?;
                Ok::<_, anyhow::Error>(pool)
            })
        })?;

        Ok(Self {
            pool,
            catalog_path: path.to_string(),
            _marker: PhantomData,
        })
    }

    async fn initialize_schema_async(pool: &Pool<B::DB>) -> Result<()> {
        sqlx::query(B::create_migrations_table_sql())
            .execute(pool)
            .await?;

        sqlx::query(B::create_connections_table_sql())
            .execute(pool)
            .await?;

        sqlx::query(B::create_tables_table_sql())
            .execute(pool)
            .await?;

        Self::run_migrations_async(pool).await?;

        Ok(())
    }

    async fn run_migrations_async(pool: &Pool<B::DB>) -> Result<()> {
        let current_version: i64 = sqlx::query_scalar(B::select_max_migration_version_sql())
            .fetch_one(pool)
            .await?;

        if current_version < 1 {
            sqlx::query(B::migration_1_sql()).execute(pool).await?;
            sqlx::query(B::insert_migration_version_sql())
                .bind(1i32)
                .execute(pool)
                .await?;
        }

        if current_version < 2 {
            // Ignore error if column already exists
            let _ = sqlx::query(B::migration_2_sql()).execute(pool).await;
            sqlx::query(B::insert_migration_version_sql())
                .bind(2i32)
                .execute(pool)
                .await?;
        }

        Ok(())
    }

    fn block_on<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }

    fn map_connection_row(row: &<B::DB as Database>::Row) -> ConnectionInfo {
        ConnectionInfo {
            id: row.get(0),
            name: row.get(1),
            source_type: row.get(2),
            config_json: row.get(3),
        }
    }

    fn map_table_row(row: &<B::DB as Database>::Row) -> TableInfo {
        TableInfo {
            id: row.get(0),
            connection_id: row.get(1),
            schema_name: row.get(2),
            table_name: row.get(3),
            parquet_path: row.get(4),
            state_path: row.get(5),
            last_sync: row.get(6),
            arrow_schema_json: row.get(7),
        }
    }
}

impl<B: SqlxBackend> CatalogManager for SqlxCatalogManager<B>
where
    for<'c> &'c Pool<B::DB>: Executor<'c, Database = B::DB>,
    for<'r> i32: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> i64: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> String: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> Option<String>: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> &'r str: sqlx::Encode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> i32: sqlx::Encode<'r, B::DB>,
    <B::DB as Database>::Row: Row,
    usize: sqlx::ColumnIndex<<B::DB as Database>::Row>,
{
    fn close(&self) -> Result<()> {
        Ok(())
    }

    fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.block_on(async {
            let rows = sqlx::query(B::list_connections_sql())
                .fetch_all(&self.pool)
                .await?;

            Ok(rows.iter().map(Self::map_connection_row).collect())
        })
    }

    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32> {
        self.block_on(async {
            sqlx::query(B::insert_connection_sql())
                .bind(name)
                .bind(source_type)
                .bind(config_json)
                .execute(&self.pool)
                .await?;

            let id: i32 = sqlx::query_scalar(B::select_connection_id_by_name_sql())
                .bind(name)
                .fetch_one(&self.pool)
                .await?;

            Ok(id)
        })
    }

    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.block_on(async {
            let row = sqlx::query(B::select_connection_by_name_sql())
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;

            Ok(row.as_ref().map(Self::map_connection_row))
        })
    }

    fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.block_on(async {
            sqlx::query(B::upsert_table_sql())
                .bind(connection_id)
                .bind(schema_name)
                .bind(table_name)
                .bind(arrow_schema_json)
                .execute(&self.pool)
                .await?;

            let id: i32 = sqlx::query_scalar(B::select_table_id_by_key_sql())
                .bind(connection_id)
                .bind(schema_name)
                .bind(table_name)
                .fetch_one(&self.pool)
                .await?;

            Ok(id)
        })
    }

    fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        self.block_on(async {
            let rows = if let Some(id) = connection_id {
                sqlx::query(B::list_tables_by_connection_sql())
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
            } else {
                sqlx::query(B::list_tables_all_sql())
                    .fetch_all(&self.pool)
                    .await?
            };

            Ok(rows.iter().map(Self::map_table_row).collect())
        })
    }

    fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        self.block_on(async {
            let row = sqlx::query(B::select_table_by_key_sql())
                .bind(connection_id)
                .bind(schema_name)
                .bind(table_name)
                .fetch_optional(&self.pool)
                .await?;

            Ok(row.as_ref().map(Self::map_table_row))
        })
    }

    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()> {
        self.block_on(async {
            sqlx::query(B::update_table_sync_sql())
                .bind(parquet_path)
                .bind(state_path)
                .bind(table_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        })
    }

    fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let table = self
            .get_table(connection_id, schema_name, table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}.{}' not found", schema_name, table_name))?;

        self.block_on(async {
            sqlx::query(B::clear_table_cache_sql())
                .bind(table.id)
                .execute(&self.pool)
                .await?;
            Ok(table)
        })
    }

    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        let conn = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        self.block_on(async {
            sqlx::query(B::clear_connection_cache_sql())
                .bind(conn.id)
                .execute(&self.pool)
                .await?;
            Ok(())
        })
    }

    fn delete_connection(&self, name: &str) -> Result<()> {
        let conn = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        self.block_on(async {
            sqlx::query(B::delete_tables_by_connection_sql())
                .bind(conn.id)
                .execute(&self.pool)
                .await?;

            sqlx::query(B::delete_connection_by_id_sql())
                .bind(conn.id)
                .execute(&self.pool)
                .await?;

            Ok(())
        })
    }
}

impl<B: SqlxBackend> Debug for SqlxCatalogManager<B>
where
    for<'c> &'c Pool<B::DB>: Executor<'c, Database = B::DB>,
    for<'r> i32: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> i64: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> String: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> Option<String>: sqlx::Decode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> &'r str: sqlx::Encode<'r, B::DB> + sqlx::Type<B::DB>,
    for<'r> i32: sqlx::Encode<'r, B::DB>,
    <B::DB as Database>::Row: Row,
    usize: sqlx::ColumnIndex<<B::DB as Database>::Row>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlxCatalogManager")
            .field("catalog_path", &self.catalog_path)
            .finish()
    }
}

// ============================================================================
// Type aliases for convenience
// ============================================================================

pub type PostgresCatalogManager = SqlxCatalogManager<PostgresBackend>;
pub type SqliteCatalogManager = SqlxCatalogManager<SqliteBackend>;