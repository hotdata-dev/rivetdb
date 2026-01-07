# Snowflake Data Fetch Integration Design

## Overview

Add Snowflake as a data fetch integration using the `snowflake-api` crate, following the existing patterns for PostgreSQL and DuckDB integrations.

## Design Decisions

- **Driver**: `snowflake-api` crate - async-native, returns Arrow directly, no external driver installation
- **Authentication**: Password and RSA key-pair (production standard for service accounts)
- **Scope**: Database required, optional schema filter for discovery
- **Warehouse**: Required (explicit is better than implicit)

## Changes

### 1. Dependencies (Cargo.toml)

```toml
snowflake-api = "0.11"
```

### 2. Source Definition (src/source.rs)

Add optional `schema` field to filter discovery:

```rust
Snowflake {
    account: String,
    user: String,
    warehouse: String,
    database: String,
    schema: Option<String>,  // NEW
    role: Option<String>,
    credential: Credential,
}
```

### 3. New Module (src/datafetch/native/snowflake.rs)

Public interface:
- `discover_tables(source, secrets) -> Result<Vec<TableMetadata>>`
- `fetch_table(source, secrets, catalog, schema, table, writer) -> Result<()>`

**Authentication flow:**
- Credential JSON with `{"password": "..."}` for password auth
- Credential JSON with `{"private_key": "..."}` for key-pair auth

**Discovery query:**
```sql
SELECT table_catalog, table_schema, table_name, table_type,
       column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_schema != 'INFORMATION_SCHEMA'
  AND table_catalog = '<database>'
  [AND table_schema = '<schema>']
ORDER BY table_catalog, table_schema, table_name, ordinal_position
```

**Type mapping (Snowflake → Arrow):**
| Snowflake | Arrow |
|-----------|-------|
| NUMBER/DECIMAL | Decimal128 or Int64 |
| FLOAT/DOUBLE | Float64 |
| VARCHAR/TEXT/STRING | Utf8 |
| BOOLEAN | Boolean |
| DATE | Date32 |
| TIME | Time64 |
| TIMESTAMP_NTZ | Timestamp(Microsecond, None) |
| TIMESTAMP_LTZ/TZ | Timestamp(Microsecond, Some(tz)) |
| BINARY | Binary |
| VARIANT/OBJECT/ARRAY | LargeUtf8 (JSON) |

### 4. Dispatcher Update (src/datafetch/native/mod.rs)

Route `Source::Snowflake` to the new implementation instead of returning `UnsupportedDriver`.

### 5. Testing

- Type mapping unit tests
- Credential parsing unit tests
- Integration test stub (skipped by default, requires credentials)
