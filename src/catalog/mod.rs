mod duckdb_manager;
mod postgres_manager;

mod manager;

pub use duckdb_manager::DuckdbCatalogManager;
pub use manager::{CatalogManager, ConnectionInfo, TableInfo};
pub use postgres_manager::PostgresCatalogManager;
