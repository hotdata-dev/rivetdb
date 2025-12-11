use super::TableInfo;
use crate::catalog::manager::{CatalogManager, ConnectionInfo};
use anyhow::Result;
use sqlx::{Row, SqlitePool};
use std::fmt::Debug;
use tokio::task::block_in_place;

pub struct SqliteCatalogManager {
    pool: SqlitePool,
    catalog_path: String,
}

impl SqliteCatalogManager {
    pub fn new(db_path: &str) -> Result<Self> {
        let pool = block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let uri = format!("sqlite:{}?mode=rwc", db_path);
                let pool = SqlitePool::connect(&uri).await?;
                Self::initialize_schema_async(&pool).await?;
                Ok::<_, anyhow::Error>(pool)
            })
        })?;

        Ok(Self {
            pool,
            catalog_path: db_path.to_string(),
        })
    }

    async fn initialize_schema_async(pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                source_type TEXT NOT NULL,
                config_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tables (
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
            )
        "#,
        )
        .execute(pool)
        .await?;

        Self::run_migrations_async(pool).await?;

        Ok(())
    }

    async fn run_migrations_async(pool: &SqlitePool) -> Result<()> {
        let current_version: i64 =
            sqlx::query_scalar(r#"SELECT COALESCE(MAX(version), 0) FROM schema_migrations"#)
                .fetch_one(pool)
                .await?;

        // Migration 1: inject "type":"postgres" if missing
        if current_version < 1 {
            sqlx::query(
                r#"
                UPDATE connections
                SET config_json = json_patch(config_json, '{"type":"postgres"}')
                WHERE json_extract(config_json, '$.type') IS NULL
            "#,
            )
            .execute(pool)
            .await?;

            sqlx::query("INSERT INTO schema_migrations (version) VALUES (1)")
                .execute(pool)
                .await?;
        }

        // Migration 2: add arrow_schema_json if missing
        if current_version < 2 {
            sqlx::query(r#"ALTER TABLE tables ADD COLUMN arrow_schema_json TEXT"#)
                .execute(pool)
                .await
                .ok(); // ignore if already exists

            sqlx::query("INSERT INTO schema_migrations (version) VALUES (2)")
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
}

impl CatalogManager for SqliteCatalogManager {
    fn close(&self) -> Result<()> {
        // sqlx pools do not need explicit close
        Ok(())
    }

    fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.block_on(async {
            let rows = sqlx::query(
                r#"
                SELECT id, name, source_type, config_json
                FROM connections
                ORDER BY name
            "#,
            )
            .fetch_all(&self.pool)
            .await?;

            let connections = rows
                .iter()
                .map(|row| ConnectionInfo {
                    id: row.get("id"),
                    name: row.get("name"),
                    source_type: row.get("source_type"),
                    config_json: row.get("config_json"),
                })
                .collect();

            Ok(connections)
        })
    }

    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32> {
        self.block_on(async {
            sqlx::query(
                r#"
                INSERT INTO connections (name, source_type, config_json)
                VALUES (?, ?, ?)
            "#,
            )
            .bind(name)
            .bind(source_type)
            .bind(config_json)
            .execute(&self.pool)
            .await?;

            let id: i32 = sqlx::query_scalar(r#"SELECT id FROM connections WHERE name = ?"#)
                .bind(name)
                .fetch_one(&self.pool)
                .await?;

            Ok(id)
        })
    }

    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.block_on(async {
            let row = sqlx::query(
                r#"
                SELECT id, name, source_type, config_json
                FROM connections
                WHERE name = ?
            "#,
            )
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

            Ok(row.map(|r| ConnectionInfo {
                id: r.get("id"),
                name: r.get("name"),
                source_type: r.get("source_type"),
                config_json: r.get("config_json"),
            }))
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
            sqlx::query(
                r#"
                INSERT INTO tables (connection_id, schema_name, table_name, arrow_schema_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (connection_id, schema_name, table_name)
                DO UPDATE SET arrow_schema_json = excluded.arrow_schema_json
            "#,
            )
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .bind(arrow_schema_json)
            .execute(&self.pool)
            .await?;

            let id: i32 = sqlx::query_scalar(
                r#"
                SELECT id FROM tables
                WHERE connection_id = ? AND schema_name = ? AND table_name = ?
            "#,
            )
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
                sqlx::query(
                    r#"
                    SELECT id, connection_id, schema_name, table_name,
                           parquet_path, state_path, last_sync, arrow_schema_json
                    FROM tables
                    WHERE connection_id = ?
                    ORDER BY schema_name, table_name
                "#,
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            } else {
                sqlx::query(
                    r#"
                    SELECT id, connection_id, schema_name, table_name,
                           parquet_path, state_path, last_sync, arrow_schema_json
                    FROM tables
                    ORDER BY schema_name, table_name
                "#,
                )
                .fetch_all(&self.pool)
                .await?
            };

            let tables = rows
                .iter()
                .map(|row| TableInfo {
                    id: row.get("id"),
                    connection_id: row.get("connection_id"),
                    schema_name: row.get("schema_name"),
                    table_name: row.get("table_name"),
                    parquet_path: row.get("parquet_path"),
                    state_path: row.get("state_path"),
                    last_sync: row.get("last_sync"),
                    arrow_schema_json: row.get("arrow_schema_json"),
                })
                .collect();

            Ok(tables)
        })
    }

    fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        self.block_on(async {
            let row = sqlx::query(
                r#"
                SELECT id, connection_id, schema_name, table_name,
                       parquet_path, state_path, last_sync, arrow_schema_json
                FROM tables
                WHERE connection_id = ? AND schema_name = ? AND table_name = ?
            "#,
            )
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await?;

            Ok(row.map(|r| TableInfo {
                id: r.get("id"),
                connection_id: r.get("connection_id"),
                schema_name: r.get("schema_name"),
                table_name: r.get("table_name"),
                parquet_path: r.get("parquet_path"),
                state_path: r.get("state_path"),
                last_sync: r.get("last_sync"),
                arrow_schema_json: r.get("arrow_schema_json"),
            }))
        })
    }

    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()> {
        self.block_on(async {
            sqlx::query(
                r#"
                UPDATE tables
                SET parquet_path = ?, state_path = ?, last_sync = CURRENT_TIMESTAMP
                WHERE id = ?
            "#,
            )
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
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        self.block_on(async {
            sqlx::query(
                r#"
                UPDATE tables
                SET parquet_path = NULL,
                    state_path   = NULL,
                    last_sync    = NULL
                WHERE id = ?
            "#,
            )
            .bind(table.id)
            .execute(&self.pool)
            .await?;

            Ok(table)
        })
    }

    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        let conn = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        self.block_on(async {
            sqlx::query(
                r#"
                UPDATE tables
                SET parquet_path = NULL,
                    state_path = NULL,
                    last_sync = NULL
                WHERE connection_id = ?
            "#,
            )
            .bind(conn.id)
            .execute(&self.pool)
            .await?;

            Ok(())
        })
    }

    fn delete_connection(&self, name: &str) -> Result<()> {
        let conn = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        self.block_on(async {
            sqlx::query("DELETE FROM tables WHERE connection_id = ?")
                .bind(conn.id)
                .execute(&self.pool)
                .await?;

            sqlx::query("DELETE FROM connections WHERE id = ?")
                .bind(conn.id)
                .execute(&self.pool)
                .await?;

            Ok(())
        })
    }
}

impl Debug for SqliteCatalogManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteCatalogManager")
            .field("catalog_path", &self.catalog_path)
            .finish()
    }
}