use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use anyhow::Result;
use deadpool_postgres::{Config, Pool, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;
use tokio::task::block_in_place;
use tokio_postgres::config::Config as PgConfig;

pub struct PostgresCatalogManager {
    pool: Pool,
}

impl PostgresCatalogManager {
    pub fn new(connection_string: &str) -> Result<Self> {
        // Parse the connection string to extract config
        let pg_config = PgConfig::from_str(connection_string)?;

        // Build deadpool config from parsed postgres config
        let mut cfg = Config::new();
        cfg.host = pg_config.get_hosts().first().map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => s.clone(),
            tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().to_string(),
        });
        cfg.port = pg_config.get_ports().first().copied();
        cfg.user = pg_config.get_user().map(|s| s.to_string());
        cfg.password = pg_config
            .get_password()
            .map(|s| String::from_utf8_lossy(s).to_string());
        cfg.dbname = pg_config.get_dbname().map(|s| s.to_string());

        // Create TLS connector
        let tls_connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()?;
        let tls = MakeTlsConnector::new(tls_connector);

        let pool = cfg.create_pool(Some(Runtime::Tokio1), tls)?;

        // Initialize schema
        block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = pool.get().await?;
                Self::initialize_schema_async(&client).await
            })
        })?;

        Ok(Self { pool })
    }

    async fn initialize_schema_async(client: &deadpool_postgres::Client) -> Result<()> {
        // Create connections table
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS connections (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                source_type TEXT NOT NULL,
                config_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
                &[],
            )
            .await?;

        // Create tables table
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS tables (
                id SERIAL PRIMARY KEY,
                connection_id INTEGER NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                parquet_path TEXT,
                state_path TEXT,
                last_sync TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (connection_id) REFERENCES connections(id),
                UNIQUE (connection_id, schema_name, table_name)
            )",
                &[],
            )
            .await?;

        Ok(())
    }

    fn block_on<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }
}

impl CatalogManager for PostgresCatalogManager {
    fn close(&self) -> Result<()> {
        // deadpool handles connection cleanup automatically
        Ok(())
    }

    fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.block_on(async {
            let client = self.pool.get().await?;

            let rows = client
                .query(
                    "SELECT id, name, source_type, config_json FROM connections ORDER BY name",
                    &[],
                )
                .await?;

            let connections = rows
                .iter()
                .map(|row| ConnectionInfo {
                    id: row.get(0),
                    name: row.get(1),
                    source_type: row.get(2),
                    config_json: row.get(3),
                })
                .collect();

            Ok(connections)
        })
    }

    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32> {
        self.block_on(async {
            let client = self.pool.get().await?;

            let row = client.query_one(
                "INSERT INTO connections (name, source_type, config_json) VALUES ($1, $2, $3) RETURNING id",
                &[&name, &source_type, &config_json],
            ).await?;

            Ok(row.get(0))
        })
    }

    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.block_on(async {
            let client = self.pool.get().await?;

            let rows = client
                .query(
                    "SELECT id, name, source_type, config_json FROM connections WHERE name = $1",
                    &[&name],
                )
                .await?;

            if let Some(row) = rows.first() {
                Ok(Some(ConnectionInfo {
                    id: row.get(0),
                    name: row.get(1),
                    source_type: row.get(2),
                    config_json: row.get(3),
                }))
            } else {
                Ok(None)
            }
        })
    }

    fn add_table(&self, connection_id: i32, schema_name: &str, table_name: &str) -> Result<i32> {
        self.block_on(async {
            let client = self.pool.get().await?;

            client.execute(
                "INSERT INTO tables (connection_id, schema_name, table_name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                &[&connection_id, &schema_name, &table_name],
            ).await?;

            let row = client.query_one(
                "SELECT id FROM tables WHERE connection_id = $1 AND schema_name = $2 AND table_name = $3",
                &[&connection_id, &schema_name, &table_name],
            ).await?;

            Ok(row.get(0))
        })
    }

    fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        self.block_on(async {
            let client = self.pool.get().await?;

            let rows = if let Some(conn_id) = connection_id {
                client.query(
                    "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                     CAST(last_sync AS VARCHAR) as last_sync \
                     FROM tables WHERE connection_id = $1 ORDER BY schema_name, table_name",
                    &[&conn_id],
                ).await?
            } else {
                client.query(
                    "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                     CAST(last_sync AS VARCHAR) as last_sync \
                     FROM tables ORDER BY schema_name, table_name",
                    &[],
                ).await?
            };

            let tables = rows
                .iter()
                .map(|row| TableInfo {
                    id: row.get(0),
                    connection_id: row.get(1),
                    schema_name: row.get(2),
                    table_name: row.get(3),
                    parquet_path: row.get(4),
                    state_path: row.get(5),
                    last_sync: row.get(6),
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
            let client = self.pool.get().await?;

            let rows = client
                .query(
                    "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                 CAST(last_sync AS VARCHAR) as last_sync \
                 FROM tables WHERE connection_id = $1 AND schema_name = $2 AND table_name = $3",
                    &[&connection_id, &schema_name, &table_name],
                )
                .await?;

            if let Some(row) = rows.first() {
                Ok(Some(TableInfo {
                    id: row.get(0),
                    connection_id: row.get(1),
                    schema_name: row.get(2),
                    table_name: row.get(3),
                    parquet_path: row.get(4),
                    state_path: row.get(5),
                    last_sync: row.get(6),
                }))
            } else {
                Ok(None)
            }
        })
    }

    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()> {
        self.block_on(async {
            let client = self.pool.get().await?;

            client.execute(
                "UPDATE tables SET parquet_path = $1, state_path = $2, last_sync = CURRENT_TIMESTAMP WHERE id = $3",
                &[&parquet_path, &state_path, &table_id],
            ).await?;
            Ok(())
        })
    }

    fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        // Get table info before clearing
        let table_info = self
            .get_table(connection_id, schema_name, table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}.{}' not found", schema_name, table_name))?;

        // Update table entry to NULL out paths and sync time
        self.block_on(async {
            let client = self.pool.get().await?;

            client.execute(
                "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE id = $1",
                &[&table_info.id],
            ).await?;

            Ok(table_info)
        })
    }

    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        // Get connection info
        let conn_info = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Update all table entries to NULL out paths and sync time
        self.block_on(async {
            let client = self.pool.get().await?;

            client.execute(
                "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE connection_id = $1",
                &[&conn_info.id],
            ).await?;

            Ok(())
        })
    }

    fn delete_connection(&self, name: &str) -> Result<()> {
        // Get connection info
        let conn_info = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Delete from database
        self.block_on(async {
            let client = self.pool.get().await?;

            client
                .execute(
                    "DELETE FROM tables WHERE connection_id = $1",
                    &[&conn_info.id],
                )
                .await?;

            client
                .execute("DELETE FROM connections WHERE id = $1", &[&conn_info.id])
                .await?;

            Ok(())
        })
    }
}

impl Debug for PostgresCatalogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresCatalogManager")
            .field("connection_string", &"<redacted>")
            .field("pool", &"<deadpool Pool>")
            .finish()
    }
}
