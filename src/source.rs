use serde::{Deserialize, Serialize};

/// Represents a data source connection with typed configuration.
/// The `type` field is used as the JSON discriminator via serde's tag attribute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        password: String,
        database: String,
    },
    Snowflake {
        account: String,
        user: String,
        password: String,
        warehouse: String,
        database: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
    },
    Motherduck {
        token: String,
        database: String,
    },
}

impl Source {
    /// Returns the source type as a string (e.g., "postgres", "snowflake", "motherduck")
    pub fn source_type(&self) -> &'static str {
        match self {
            Source::Postgres { .. } => "postgres",
            Source::Snowflake { .. } => "snowflake",
            Source::Motherduck { .. } => "motherduck",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_serialization() {
        let source = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "secret".to_string(),
            database: "mydb".to_string(),
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"postgres""#));
        assert!(json.contains(r#""host":"localhost""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_snowflake_serialization() {
        let source = Source::Snowflake {
            account: "xyz123".to_string(),
            user: "bob".to_string(),
            password: "secret".to_string(),
            warehouse: "COMPUTE_WH".to_string(),
            database: "PROD".to_string(),
            role: Some("ANALYST".to_string()),
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"snowflake""#));
        assert!(json.contains(r#""account":"xyz123""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_snowflake_without_role() {
        let source = Source::Snowflake {
            account: "xyz123".to_string(),
            user: "bob".to_string(),
            password: "secret".to_string(),
            warehouse: "COMPUTE_WH".to_string(),
            database: "PROD".to_string(),
            role: None,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains("role"));
    }

    #[test]
    fn test_motherduck_serialization() {
        let source = Source::Motherduck {
            token: "md_abc123".to_string(),
            database: "my_db".to_string(),
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"motherduck""#));
        assert!(json.contains(r#""token":"md_abc123""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_source_type_method() {
        let postgres = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            password: "p".to_string(),
            database: "d".to_string(),
        };
        assert_eq!(postgres.source_type(), "postgres");

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            password: "p".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            role: None,
        };
        assert_eq!(snowflake.source_type(), "snowflake");

        let motherduck = Source::Motherduck {
            token: "t".to_string(),
            database: "d".to_string(),
        };
        assert_eq!(motherduck.source_type(), "motherduck");
    }
}
