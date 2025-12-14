mod backend;
mod migrations;
mod postgres_manager;
mod sqlite_manager;

mod manager;

pub use manager::{block_on, CatalogManager, ConnectionInfo, TableInfo};
pub use postgres_manager::PostgresCatalogManager;
pub use sqlite_manager::SqliteCatalogManager;
