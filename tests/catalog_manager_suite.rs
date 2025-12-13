use rivetdb::catalog::{CatalogManager, PostgresCatalogManager, SqliteCatalogManager};
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

struct CatalogTestContext<M, G = ()> {
    manager: M,
    _guard: G,
}

impl<M, G> CatalogTestContext<M, G> {
    fn new(manager: M, guard: G) -> Self {
        Self {
            manager,
            _guard: guard,
        }
    }
}

impl<M: CatalogManager, G> CatalogTestContext<M, G> {
    fn manager(&self) -> &M {
        &self.manager
    }
}

async fn create_sqlite_catalog() -> CatalogTestContext<SqliteCatalogManager, TempDir> {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db_path = dir.path().join("catalog.sqlite");
    let manager = SqliteCatalogManager::new(db_path.to_str().unwrap()).unwrap();
    manager.run_migrations().unwrap();
    CatalogTestContext::new(manager, dir)
}

async fn create_postgres_catalog(
) -> CatalogTestContext<PostgresCatalogManager, testcontainers::ContainerAsync<Postgres>> {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("Failed to start postgres container");

    let host_port = container.get_host_port_ipv4(5432).await.unwrap();
    let connection_string = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        host_port
    );

    let manager = PostgresCatalogManager::new(&connection_string).unwrap();
    manager.run_migrations().unwrap();

    CatalogTestContext::new(manager, container)
}

macro_rules! catalog_manager_tests {
    ($module:ident, $setup_fn:ident) => {
        mod $module {
            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            async fn catalog_initialization() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let connections = catalog.list_connections().unwrap();
                assert!(connections.is_empty());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;

                catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();

                let connections = catalog.list_connections().unwrap();
                assert_eq!(connections.len(), 1);
                assert_eq!(connections[0].name, "test_db");
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn get_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();

                let conn = catalog.get_connection("test_db").unwrap();
                assert!(conn.is_some());
                assert_eq!(conn.unwrap().name, "test_db");

                let missing = catalog.get_connection("missing").unwrap();
                assert!(missing.is_none());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_table() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;

                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();

                let first_id = catalog.add_table(conn_id, "public", "users", "").unwrap();
                let second_id = catalog.add_table(conn_id, "public", "users", "").unwrap();
                assert_eq!(first_id, second_id);
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn get_table() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                catalog.add_table(conn_id, "public", "users", "").unwrap();

                let table = catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .unwrap();
                assert_eq!(table.schema_name, "public");
                assert_eq!(table.table_name, "users");

                let missing = catalog.get_table(conn_id, "public", "missing").unwrap();
                assert!(missing.is_none());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn update_table_sync() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                let table_id = catalog.add_table(conn_id, "public", "users", "").unwrap();

                catalog
                    .update_table_sync(table_id, "/path/to/data.parquet", "/path/to/state.json")
                    .unwrap();

                let table = catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .unwrap();
                assert_eq!(table.parquet_path.as_deref(), Some("/path/to/data.parquet"));
                assert_eq!(table.state_path.as_deref(), Some("/path/to/state.json"));
                assert!(table.last_sync.is_some());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn list_tables_multiple_connections() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config1 = r#"{"host": "localhost", "port": 5432, "database": "db1"}"#;
                let conn1 = catalog
                    .add_connection("neon_east", "postgres", config1)
                    .unwrap();
                catalog.add_table(conn1, "public", "cities", "").unwrap();
                catalog.add_table(conn1, "public", "locations", "").unwrap();
                catalog.add_table(conn1, "public", "table_1", "").unwrap();

                let config2 = r#"{"host": "localhost", "port": 5432, "database": "db2"}"#;
                let conn2 = catalog
                    .add_connection("connection2", "postgres", config2)
                    .unwrap();
                catalog.add_table(conn2, "public", "table_1", "").unwrap();

                let all_tables = catalog.list_tables(None).unwrap();
                assert_eq!(all_tables.len(), 4);

                let conn1_tables = catalog.list_tables(Some(conn1)).unwrap();
                assert_eq!(conn1_tables.len(), 3);
                assert!(conn1_tables.iter().all(|t| t.connection_id == conn1));

                let conn2_tables = catalog.list_tables(Some(conn2)).unwrap();
                assert_eq!(conn2_tables.len(), 1);
                assert!(conn2_tables.iter().all(|t| t.connection_id == conn2));
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn list_tables_with_cached_status() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                let cached_id = catalog
                    .add_table(conn_id, "public", "cached_table", "")
                    .unwrap();
                catalog
                    .add_table(conn_id, "public", "not_cached_table", "")
                    .unwrap();

                catalog
                    .update_table_sync(cached_id, "/fake/path/test.parquet", "/fake/path/test.json")
                    .unwrap();

                let tables = catalog.list_tables(Some(conn_id)).unwrap();
                assert_eq!(tables.len(), 2);

                let cached = tables
                    .iter()
                    .find(|t| t.table_name == "cached_table")
                    .unwrap();
                let not_cached = tables
                    .iter()
                    .find(|t| t.table_name == "not_cached_table")
                    .unwrap();

                assert!(cached.parquet_path.is_some());
                assert!(cached.state_path.is_some());
                assert!(cached.last_sync.is_some());

                assert!(not_cached.parquet_path.is_none());
                assert!(not_cached.state_path.is_none());
                assert!(not_cached.last_sync.is_none());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn clear_connection_cache_metadata() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                let table_id = catalog.add_table(conn_id, "public", "users", "").unwrap();

                catalog
                    .update_table_sync(table_id, "/fake/path/test.parquet", "/fake/path/test.json")
                    .unwrap();

                catalog.clear_connection_cache_metadata("test_db").unwrap();
                let table = catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .unwrap();
                assert!(table.parquet_path.is_none());
                assert!(table.state_path.is_none());
                assert!(table.last_sync.is_none());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn delete_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                catalog.add_table(conn_id, "public", "users", "").unwrap();

                assert!(catalog.get_connection("test_db").unwrap().is_some());
                assert!(catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .is_some());

                catalog.delete_connection("test_db").unwrap();

                assert!(catalog.get_connection("test_db").unwrap().is_none());
                assert!(catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .is_none());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn clear_nonexistent_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let err = catalog.clear_connection_cache_metadata("missing");
                assert!(err.is_err());
                assert!(err.unwrap_err().to_string().contains("not found"));
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn delete_nonexistent_connection() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                let err = catalog.delete_connection("missing");
                assert!(err.is_err());
                assert!(err.unwrap_err().to_string().contains("not found"));
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn clear_table_cache_metadata() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                let users_id = catalog.add_table(conn_id, "public", "users", "").unwrap();
                let orders_id = catalog.add_table(conn_id, "public", "orders", "").unwrap();

                catalog
                    .update_table_sync(users_id, "/fake/users.parquet", "/fake/users.json")
                    .unwrap();
                catalog
                    .update_table_sync(orders_id, "/fake/orders.parquet", "/fake/orders.json")
                    .unwrap();

                let table_info = catalog
                    .clear_table_cache_metadata(conn_id, "public", "users")
                    .unwrap();
                assert!(table_info.parquet_path.is_some());

                let users_after = catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .unwrap();
                assert!(users_after.parquet_path.is_none());
                assert!(users_after.state_path.is_none());
                assert!(users_after.last_sync.is_none());

                let orders_after = catalog
                    .get_table(conn_id, "public", "orders")
                    .unwrap()
                    .unwrap();
                assert!(orders_after.parquet_path.is_some());
                assert!(orders_after.state_path.is_some());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn clear_table_cache_metadata_nonexistent() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();

                let err = catalog.clear_table_cache_metadata(conn_id, "public", "missing");
                assert!(err.is_err());
                assert!(err.unwrap_err().to_string().contains("not found"));
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn clear_table_without_cache() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();

                let config = r#"{"host": "localhost", "port": 5432, "database": "test"}"#;
                let conn_id = catalog
                    .add_connection("test_db", "postgres", config)
                    .unwrap();
                catalog.add_table(conn_id, "public", "users", "").unwrap();

                let table_info = catalog
                    .clear_table_cache_metadata(conn_id, "public", "users")
                    .unwrap();
                assert!(table_info.parquet_path.is_none());
                assert!(table_info.state_path.is_none());

                let table_after = catalog
                    .get_table(conn_id, "public", "users")
                    .unwrap()
                    .unwrap();
                assert!(table_after.parquet_path.is_none());
                assert!(table_after.state_path.is_none());
                assert!(table_after.last_sync.is_none());
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn close_is_idempotent() {
                let ctx = super::$setup_fn().await;
                let catalog = ctx.manager();
                catalog.close().unwrap();
                catalog.close().unwrap();
                catalog.close().unwrap();
            }
        }
    };
}

catalog_manager_tests!(sqlite, create_sqlite_catalog);
catalog_manager_tests!(postgres, create_postgres_catalog);
