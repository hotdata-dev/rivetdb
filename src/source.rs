use crate::secrets::SecretManager;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

/// Credential storage - either no credential or a reference to a stored secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    #[default]
    None,
    SecretRef {
        name: String,
    },
}

/// Resolved credential - a borrowed view of plaintext bytes.
/// Only exists for the duration of the closure passed to `with_resolved_credential`.
pub enum ResolvedCredential<'a> {
    None,
    Bytes(&'a [u8]),
}

impl<'a> ResolvedCredential<'a> {
    /// Interpret the credential as UTF-8 text.
    pub fn as_str(&self) -> anyhow::Result<&'a str> {
        match self {
            ResolvedCredential::None => Err(anyhow::anyhow!("no credential available")),
            ResolvedCredential::Bytes(bytes) => {
                std::str::from_utf8(bytes).map_err(|_| anyhow::anyhow!("credential is not valid UTF-8"))
            }
        }
    }

    /// Access the raw bytes, if present.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self {
            ResolvedCredential::None => None,
            ResolvedCredential::Bytes(bytes) => Some(bytes),
        }
    }
}

/// Represents a data source connection with typed configuration.
/// The `type` field is used as the JSON discriminator via serde's tag attribute.
///
/// Credentials are stored as secrets and referenced via the `credential` field.
/// At connection time, use `with_resolved_credential` to access plaintext.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    Snowflake {
        account: String,
        user: String,
        warehouse: String,
        database: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        #[serde(default)]
        credential: Credential,
    },
    Motherduck {
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    Duckdb {
        path: String,
    },
}

impl Source {
    /// Returns the source type as a string (e.g., "postgres", "snowflake", "motherduck", "duckdb")
    pub fn source_type(&self) -> &'static str {
        match self {
            Source::Postgres { .. } => "postgres",
            Source::Snowflake { .. } => "snowflake",
            Source::Motherduck { .. } => "motherduck",
            Source::Duckdb { .. } => "duckdb",
        }
    }

    /// Returns the catalog name for this source, if applicable.
    /// For Motherduck, this is the database name used to filter table discovery.
    pub fn catalog(&self) -> Option<&str> {
        match self {
            Source::Motherduck { database, .. } => Some(database.as_str()),
            _ => None,
        }
    }

    /// Access the credential field.
    pub fn credential(&self) -> &Credential {
        match self {
            Source::Postgres { credential, .. } => credential,
            Source::Snowflake { credential, .. } => credential,
            Source::Motherduck { credential, .. } => credential,
            Source::Duckdb { .. } => &Credential::None,
        }
    }

    /// Resolve credential and execute a closure with the plaintext.
    /// The plaintext only exists for the duration of the closure.
    pub async fn with_resolved_credential<F, R>(
        &self,
        secrets: &dyn SecretManager,
        f: F,
    ) -> anyhow::Result<R>
    where
        F: FnOnce(ResolvedCredential<'_>) -> anyhow::Result<R>,
    {
        match self.credential() {
            Credential::None => f(ResolvedCredential::None),
            Credential::SecretRef { name } => {
                let bytes = secrets.get(name).await.map_err(|e| {
                    anyhow::anyhow!("Failed to resolve secret '{}': {}", name, e)
                })?;
                let result = f(ResolvedCredential::Bytes(&bytes))?;
                // bytes dropped here - plaintext lifetime ends
                Ok(result)
            }
        }
    }

    /// Builds the connection string for this source.
    /// User-provided values are URL-encoded to prevent connection string injection.
    pub fn connection_string(&self, credential: ResolvedCredential<'_>) -> anyhow::Result<String> {
        match self {
            Source::Postgres {
                host,
                port,
                user,
                database,
                ..
            } => {
                let password = credential.as_str()?;
                Ok(format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    encode(user),
                    encode(password),
                    encode(host),
                    port,
                    encode(database)
                ))
            }
            Source::Snowflake {
                account,
                user,
                warehouse,
                database,
                ..
            } => {
                let password = credential.as_str()?;
                Ok(format!(
                    "{}:{}@{}/{}/{}",
                    encode(user),
                    encode(password),
                    encode(account),
                    encode(database),
                    encode(warehouse)
                ))
            }
            Source::Motherduck { database, .. } => {
                let token = credential.as_str()?;
                Ok(format!(
                    "md:{}?motherduck_token={}",
                    encode(database),
                    encode(token)
                ))
            }
            Source::Duckdb { path } => Ok(path.clone()),
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
            database: "mydb".to_string(),
            credential: Credential::SecretRef {
                name: "my-pg-secret".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"postgres""#));
        assert!(json.contains(r#""host":"localhost""#));
        assert!(json.contains(r#""my-pg-secret""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_snowflake_serialization() {
        let source = Source::Snowflake {
            account: "xyz123".to_string(),
            user: "bob".to_string(),
            warehouse: "COMPUTE_WH".to_string(),
            database: "PROD".to_string(),
            role: Some("ANALYST".to_string()),
            credential: Credential::SecretRef {
                name: "snowflake-secret".to_string(),
            },
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
            warehouse: "COMPUTE_WH".to_string(),
            database: "PROD".to_string(),
            role: None,
            credential: Credential::SecretRef {
                name: "secret".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains(r#""role""#));
    }

    #[test]
    fn test_motherduck_serialization() {
        let source = Source::Motherduck {
            database: "my_db".to_string(),
            credential: Credential::SecretRef {
                name: "md-token".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"motherduck""#));
        assert!(json.contains(r#""md-token""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_catalog_method() {
        let motherduck = Source::Motherduck {
            database: "my_database".to_string(),
            credential: Credential::None,
        };
        assert_eq!(motherduck.catalog(), Some("my_database"));

        let duckdb = Source::Duckdb {
            path: "/path/to/db".to_string(),
        };
        assert_eq!(duckdb.catalog(), None);

        let postgres = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(postgres.catalog(), None);

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            role: None,
            credential: Credential::None,
        };
        assert_eq!(snowflake.catalog(), None);
    }

    #[test]
    fn test_source_type_method() {
        let postgres = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(postgres.source_type(), "postgres");

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            role: None,
            credential: Credential::None,
        };
        assert_eq!(snowflake.source_type(), "snowflake");

        let motherduck = Source::Motherduck {
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(motherduck.source_type(), "motherduck");
    }

    #[test]
    fn test_connection_string_with_credential() {
        let source = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "user".to_string(),
            database: "db".to_string(),
            credential: Credential::None, // Doesn't matter for this test
        };

        let cred = ResolvedCredential::Bytes(b"secret");
        let conn_str = source.connection_string(cred).unwrap();
        assert_eq!(conn_str, "postgresql://user:secret@localhost:5432/db");
    }

    #[test]
    fn test_duckdb_connection_string_no_credential() {
        let source = Source::Duckdb {
            path: "/path/to/db.duckdb".to_string(),
        };

        let conn_str = source.connection_string(ResolvedCredential::None).unwrap();
        assert_eq!(conn_str, "/path/to/db.duckdb");
    }

    #[test]
    fn test_credential_accessor() {
        let with_secret = Source::Postgres {
            host: "h".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::SecretRef {
                name: "my-secret".to_string(),
            },
        };
        assert!(matches!(
            with_secret.credential(),
            Credential::SecretRef { name } if name == "my-secret"
        ));

        let duckdb = Source::Duckdb {
            path: "/p".to_string(),
        };
        assert!(matches!(duckdb.credential(), Credential::None));
    }
}
