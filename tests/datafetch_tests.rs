//! Integration tests for datafetch module

use rivetdb::datafetch::{ConnectionConfig, DataFetcher, NativeFetcher};

#[tokio::test]
async fn test_unsupported_driver() {
    let fetcher = NativeFetcher::new();
    let config = ConnectionConfig {
        source_type: "unsupported_db".to_string(),
        connection_string: "fake://connection".to_string(),
    };

    let result = fetcher.discover_tables(&config).await;
    assert!(result.is_err(), "Should fail for unsupported driver");
}