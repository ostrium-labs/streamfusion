//! Flink logical types mapped onto Arrow, the layout the engine's operators consume.
//!
//! The mapping mirrors the engine's own transport, `tech.streamfusion.arrow.ArrowConversion`
//! (its `TypeConverter` and `toArrowField`), with its defaults (`binaryElt` on, so `BINARY(n)` is
//! variable-width). It is not the mapping the FC1 plan first sketched (`TIMESTAMP(p)` as an Arrow
//! `Timestamp`): the operators read timestamps as a two-component struct, which keeps Flink's
//! full range (years 0000–9999 at nanosecond precision) that an `i64` of nanoseconds cannot hold.
//! FC1 ruling R4.3 records the change.
//!
//! | Flink logical type | Arrow type |
//! |---|---|
//! | `BOOLEAN` | `Boolean` |
//! | `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `Int8`, `Int16`, `Int32`, `Int64` |
//! | `FLOAT`, `DOUBLE` | `Float32`, `Float64` |
//! | `CHAR(n)`, `VARCHAR(n)`, `STRING` | `Utf8` |
//! | `BINARY(n)`, `VARBINARY(n)`, `BYTES` | `Binary` |
//! | `DECIMAL(p, s)` | `Decimal128(p, s)` (Flink's precision is at most 38, so never `Decimal256`) |
//! | `DATE` | `Date32` (days since the epoch) |
//! | `TIME(p)` | `Time32(Millisecond)` (Flink keeps `TIME` at millisecond precision at runtime) |
//! | `TIMESTAMP(p)`, `TIMESTAMP_LTZ(p)` | `Struct<millis: Int64, nano_of_milli: Int32>`, both components non-null and tagged with `streamfusion.timestamp.component` metadata |
//! | `INTERVAL YEAR TO MONTH` (all resolutions) | `Int32` (months) |
//! | `INTERVAL DAY TO SECOND` (all resolutions) | `Int64` (milliseconds) |
//! | `ARRAY<t>` | `List<element: t>` |
//! | `MAP<k, v>` | `Map<items: Struct<key: k NOT NULL, value: v>>`, keys unsorted |
//! | `MULTISET<t>` | `Map<items: Struct<key: t NOT NULL, value: Int32 NOT NULL>>` (element to count) |
//! | `ROW<…>` | `Struct<…>` |
//! | `NULL` | `Null` |
//!
//! `RAW`, `SYMBOL`, `DESCRIPTOR`, `VARIANT` and `TIMESTAMP WITH TIME ZONE` have no column layout:
//! [`to_arrow`] refuses them, as the engine does ("Unsupported data type").

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, TimeUnit};

use super::logical::{LogicalKind, LogicalType};

/// The metadata key that marks the two components of an engine timestamp column.
pub const TIMESTAMP_COMPONENT_KEY: &str = "streamfusion.timestamp.component";

/// The engine's timestamp layout: milliseconds since the epoch plus the sub-millisecond rest.
pub fn timestamp_struct() -> DataType {
    let fields: Fields = [
        ("millis", DataType::Int64),
        ("nano_of_milli", DataType::Int32),
    ]
    .into_iter()
    .map(|(name, data_type)| {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            TIMESTAMP_COMPONENT_KEY.to_owned(),
            name.to_owned(),
        )]))
    })
    .collect();
    DataType::Struct(fields)
}

/// A logical type the engine has no column layout for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedArrowType(pub String);

impl std::fmt::Display for UnsupportedArrowType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unsupported data type {} currently.", self.0)
    }
}

impl std::error::Error for UnsupportedArrowType {}

/// The Arrow type of a column of `ty` (nullability is the field's, see [`to_arrow_field`]).
pub fn to_arrow(ty: &LogicalType) -> Result<DataType, UnsupportedArrowType> {
    Ok(match &ty.kind {
        LogicalKind::Boolean => DataType::Boolean,
        LogicalKind::TinyInt => DataType::Int8,
        LogicalKind::SmallInt => DataType::Int16,
        LogicalKind::Int => DataType::Int32,
        LogicalKind::BigInt => DataType::Int64,
        LogicalKind::Float => DataType::Float32,
        LogicalKind::Double => DataType::Float64,
        LogicalKind::Char(_) | LogicalKind::Varchar(_) => DataType::Utf8,
        LogicalKind::Binary(_) | LogicalKind::Varbinary(_) => DataType::Binary,
        LogicalKind::Decimal { precision, scale } => {
            DataType::Decimal128(*precision, i8::try_from(*scale).unwrap_or(i8::MAX))
        }
        LogicalKind::Date => DataType::Date32,
        LogicalKind::Time(_) => DataType::Time32(TimeUnit::Millisecond),
        LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. } => timestamp_struct(),
        LogicalKind::IntervalYearMonth { .. } => DataType::Int32,
        LogicalKind::IntervalDayTime { .. } => DataType::Int64,
        LogicalKind::Array(element) => {
            DataType::List(Arc::new(to_arrow_field("element", element)?))
        }
        LogicalKind::Map(key, value) => map_type(key, value)?,
        LogicalKind::Multiset(element) => map_type(element, &LogicalType::int().not_null())?,
        LogicalKind::Row(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| to_arrow_field(&f.name, &f.ty))
                .collect::<Result<Fields, _>>()?,
        ),
        LogicalKind::Null => DataType::Null,
        LogicalKind::TimestampTz(_)
        | LogicalKind::Raw { .. }
        | LogicalKind::Symbol
        | LogicalKind::Descriptor
        | LogicalKind::Variant => return Err(UnsupportedArrowType(ty.to_string())),
    })
}

/// The Arrow field for a column named `name` of type `ty`, nullable as the type is.
pub fn to_arrow_field(name: &str, ty: &LogicalType) -> Result<Field, UnsupportedArrowType> {
    Ok(Field::new(name, to_arrow(ty)?, ty.nullable))
}

fn map_type(key: &LogicalType, value: &LogicalType) -> Result<DataType, UnsupportedArrowType> {
    let items = Field::new(
        "items",
        DataType::Struct(Fields::from(vec![
            to_arrow_field("key", &key.clone().not_null())?,
            to_arrow_field("value", value)?,
        ])),
        false,
    );
    Ok(DataType::Map(Arc::new(items), false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogicalRowField;

    #[test]
    fn scalars_map_like_the_engine() {
        assert_eq!(to_arrow(&LogicalType::string()), Ok(DataType::Utf8));
        assert_eq!(
            to_arrow(&LogicalType::decimal(10, 2)),
            Ok(DataType::Decimal128(10, 2))
        );
        assert_eq!(to_arrow(&LogicalType::rowtime(3)), Ok(timestamp_struct()));
        assert_eq!(
            to_arrow(&LogicalType::timestamp_ltz(6)),
            Ok(timestamp_struct())
        );
        assert_eq!(
            to_arrow(&LogicalType::time(3)),
            Ok(DataType::Time32(TimeUnit::Millisecond))
        );
        assert_eq!(
            to_arrow(&LogicalType::interval_month()),
            Ok(DataType::Int32)
        );
        assert_eq!(
            to_arrow(&LogicalType::interval_second(3)),
            Ok(DataType::Int64)
        );
    }

    #[test]
    fn nested_types_keep_names_and_nullability() {
        let row = LogicalType::row(vec![
            LogicalRowField::new("a", LogicalType::int().not_null()),
            LogicalRowField::new("b", LogicalType::array(LogicalType::string())),
        ]);
        let DataType::Struct(fields) = to_arrow(&row).unwrap() else {
            panic!("ROW is a struct")
        };
        assert_eq!(fields[0].name(), "a");
        assert!(!fields[0].is_nullable());
        assert!(matches!(fields[1].data_type(), DataType::List(e) if e.name() == "element"));

        let DataType::Map(items, false) =
            to_arrow(&LogicalType::multiset(LogicalType::string())).unwrap()
        else {
            panic!("MULTISET is a map")
        };
        let DataType::Struct(kv) = items.data_type() else {
            panic!("map items are a struct")
        };
        assert!(!kv[0].is_nullable(), "map keys are declared non-null");
        assert_eq!(kv[1].data_type(), &DataType::Int32);
        assert!(!kv[1].is_nullable(), "multiset counts are non-null");
    }

    #[test]
    fn unsupported_types_are_refused() {
        assert!(to_arrow(&LogicalType::new(LogicalKind::Symbol)).is_err());
        assert!(to_arrow(&LogicalType::new(LogicalKind::Variant)).is_err());
    }
}
