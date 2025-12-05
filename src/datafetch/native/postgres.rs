//! PostgreSQL native driver implementation using sqlx

use sqlx::postgres::PgConnection;
use sqlx::{Connection, Row};

use crate::datafetch::{ColumnMetadata, ConnectionConfig, DataFetchError, TableMetadata};
use crate::storage::StorageManager;

/// Discover tables and columns from PostgreSQL
pub async fn discover_tables(
    config: &ConnectionConfig,
) -> Result<Vec<TableMetadata>, DataFetchError> {
    let mut conn = PgConnection::connect(&config.connection_string).await?;

    let rows = sqlx::query(
        r#"
        SELECT
            t.table_catalog,
            t.table_schema,
            t.table_name,
            t.table_type,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.ordinal_position::int
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        "#,
    )
    .fetch_all(&mut conn)
    .await?;

    let mut tables: Vec<TableMetadata> = Vec::new();

    for row in rows {
        let catalog: Option<String> = row.get(0);
        let schema: String = row.get(1);
        let table: String = row.get(2);
        let table_type: String = row.get(3);
        let col_name: String = row.get(4);
        let data_type: String = row.get(5);
        let is_nullable: String = row.get(6);
        let ordinal: i32 = row.get(7);

        let column = ColumnMetadata {
            name: col_name,
            data_type: pg_type_to_arrow(&data_type),
            nullable: is_nullable.to_uppercase() == "YES",
            ordinal_position: ordinal as i16,
        };

        // Find or create table entry
        if let Some(existing) = tables.iter_mut().find(|t| {
            t.catalog_name == catalog && t.schema_name == schema && t.table_name == table
        }) {
            existing.columns.push(column);
        } else {
            tables.push(TableMetadata {
                catalog_name: catalog,
                schema_name: schema,
                table_name: table,
                table_type,
                columns: vec![column],
            });
        }
    }

    Ok(tables)
}

/// Fetch table data and write to Parquet
pub async fn fetch_table(
    config: &ConnectionConfig,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    storage: &dyn StorageManager,
    connection_id: i32,
) -> Result<String, DataFetchError> {
    todo!("PostgreSQL fetch_table")
}

/// Convert PostgreSQL type name to Arrow DataType
fn pg_type_to_arrow(pg_type: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    let type_lower = pg_type.to_lowercase();

    match type_lower.as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "smallint" | "int2" => DataType::Int16,
        "integer" | "int" | "int4" => DataType::Int32,
        "bigint" | "int8" => DataType::Int64,
        "real" | "float4" => DataType::Float32,
        "double precision" | "float8" => DataType::Float64,
        "numeric" | "decimal" => DataType::Decimal128(38, 10),
        "character varying" | "varchar" | "text" | "character" | "char" | "bpchar" => DataType::Utf8,
        "bytea" => DataType::Binary,
        "date" => DataType::Date32,
        "time" | "time without time zone" => DataType::Time64(TimeUnit::Microsecond),
        "timestamp" | "timestamp without time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "timestamp with time zone" | "timestamptz" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "uuid" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "interval" => DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        _ => DataType::Utf8, // Default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_mapping() {
        use datafusion::arrow::datatypes::DataType;

        assert!(matches!(pg_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow("character varying"),
            DataType::Utf8
        ));
        assert!(matches!(pg_type_to_arrow("boolean"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow("bytea"), DataType::Binary));
    }
}