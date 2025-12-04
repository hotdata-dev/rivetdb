//! Arrow array to JSON serialization utilities.
//!
//! Uses arrow-json's encoder infrastructure to handle all Arrow types
//! without manual type-matching code.

use arrow_json::writer::{make_encoder, EncoderOptions, NullableEncoder};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::FieldRef;
use datafusion::arrow::error::ArrowError;
use std::sync::LazyLock;

/// Default encoder options (static to avoid lifetime issues)
static ENCODER_OPTIONS: LazyLock<EncoderOptions> = LazyLock::new(EncoderOptions::default);

/// Create an encoder for the given array and field.
///
/// The encoder handles all Arrow types internally, delegating to arrow-json's
/// battle-tested serialization logic.
///
/// # Arguments
/// * `array` - The Arrow array to encode values from
/// * `field` - The schema field (use `batch.schema().field(idx)`)
pub fn make_array_encoder<'a>(
    array: &'a dyn Array,
    field: &'a FieldRef,
) -> Result<NullableEncoder<'a>, ArrowError> {
    make_encoder(field, array, &ENCODER_OPTIONS)
}

/// Convert Arrow array value at a specific row to JSON using a pre-created encoder.
///
/// This is the efficient path when processing multiple rows - create the encoder once,
/// then call this for each row.
pub fn encode_value_at(encoder: &mut NullableEncoder, row_idx: usize) -> serde_json::Value {
    if encoder.is_null(row_idx) {
        return serde_json::Value::Null;
    }

    let mut buf = Vec::with_capacity(64);
    encoder.encode(row_idx, &mut buf);

    // Parse the JSON bytes produced by the encoder
    serde_json::from_slice(&buf).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    /// Helper to create a field for testing
    fn field(name: &str, data_type: DataType, nullable: bool) -> FieldRef {
        Arc::new(Field::new(name, data_type, nullable))
    }

    // ============================================================
    // Basic Type Tests - verify arrow-json handles core types
    // ============================================================

    #[test]
    fn integers() {
        let arr = Int64Array::from(vec![Some(42), None, Some(-100)]);
        let f = field("x", DataType::Int64, true);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::json!(42));
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::Value::Null);
        assert_eq!(encode_value_at(&mut enc, 2), serde_json::json!(-100));
    }

    #[test]
    fn floats() {
        let arr = Float64Array::from(vec![1.5, -2.25]);
        let f = field("x", DataType::Float64, false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::json!(1.5));
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::json!(-2.25));
    }

    #[test]
    fn booleans() {
        let arr = BooleanArray::from(vec![true, false]);
        let f = field("x", DataType::Boolean, false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::json!(true));
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::json!(false));
    }

    #[test]
    fn strings() {
        let arr = StringArray::from(vec!["hello", "world"]);
        let f = field("x", DataType::Utf8, false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::json!("hello"));
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::json!("world"));
    }

    #[test]
    fn string_with_unicode() {
        let arr = StringArray::from(vec!["Hello 👋", "日本語"]);
        let f = field("x", DataType::Utf8, false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::json!("Hello 👋"));
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::json!("日本語"));
    }

    #[test]
    fn nulls() {
        let arr = Int32Array::from(vec![None, Some(1), None] as Vec<Option<i32>>);
        let f = field("x", DataType::Int32, true);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::Value::Null);
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::json!(1));
        assert_eq!(encode_value_at(&mut enc, 2), serde_json::Value::Null);
    }

    // ============================================================
    // Temporal Types - dates and timestamps
    // ============================================================

    #[test]
    fn date32() {
        // 0 = 1970-01-01
        let arr = Date32Array::from(vec![0]);
        let f = field("x", DataType::Date32, false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        let result = encode_value_at(&mut enc, 0);
        assert!(result.is_string());
        assert!(result.as_str().unwrap().contains("1970-01-01"));
    }

    #[test]
    fn timestamp_second() {
        use datafusion::arrow::datatypes::TimeUnit;

        // 2000-01-01 12:30:45 UTC = 946729845
        let arr = TimestampSecondArray::from(vec![946729845i64]);
        let f = field("x", DataType::Timestamp(TimeUnit::Second, None), false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        let result = encode_value_at(&mut enc, 0);
        assert!(result.is_string());
        let ts = result.as_str().unwrap();
        assert!(ts.contains("2000-01-01"));
    }

    // ============================================================
    // Decimal Types
    // ============================================================

    #[test]
    fn decimal128() {
        let arr = Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let f = field("x", DataType::Decimal128(10, 2), false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        let result = encode_value_at(&mut enc, 0);
        // arrow-json outputs decimals as numbers (float)
        assert_eq!(result, serde_json::json!(123.45));
    }

    // ============================================================
    // Encoder Reuse Tests - verify batch processing works
    // ============================================================

    #[test]
    fn encoder_reuse_across_rows() {
        let arr = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let f = field("x", DataType::Int32, false);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        for i in 0..5 {
            let val = encode_value_at(&mut enc, i);
            assert_eq!(val, serde_json::json!(i + 1));
        }
    }

    #[test]
    fn encoder_with_nulls() {
        let arr = StringArray::from(vec![Some("a"), None, Some("c")]);
        let f = field("x", DataType::Utf8, true);
        let mut enc = make_array_encoder(&arr, &f).unwrap();

        assert_eq!(encode_value_at(&mut enc, 0), serde_json::json!("a"));
        assert_eq!(encode_value_at(&mut enc, 1), serde_json::Value::Null);
        assert_eq!(encode_value_at(&mut enc, 2), serde_json::json!("c"));
    }
}
