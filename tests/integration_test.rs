use tempfile::tempdir;
use rivetdb::catalog::CatalogManager;
use rivetdb::catalog::DuckdbCatalogManager;

#[test]
fn test_full_connection_workflow() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    let catalog = DuckdbCatalogManager::new(db_path.to_str().unwrap()).unwrap();

    // Add connection
    let config = r#"{"type": "postgres", "host": "localhost", "port": 5432}"#;
    let conn_id = catalog
        .add_connection("test_db", "postgres", config)
        .unwrap();

    // Add some tables
    catalog.add_table(conn_id, "public", "users", "").unwrap();
    catalog.add_table(conn_id, "public", "orders", "").unwrap();

    // List connections
    let connections = catalog.list_connections().unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].name, "test_db");

    // List tables
    let tables = catalog.list_tables(Some(conn_id)).unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().any(|t| t.table_name == "users"));
    assert!(tables.iter().any(|t| t.table_name == "orders"));
}
