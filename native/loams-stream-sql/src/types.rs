//! Flink's data types: the parsed syntax and the logical type system (design 82 §4.1, §7.1).
//!
//! - [`FlinkType`] is a type as written (`VARCHAR`, `TIMESTAMP(3) WITH LOCAL TIME ZONE`), kept by
//!   the parser.
//! - [`LogicalType`] is Flink's logical type (`org.apache.flink.table.types.logical`) the binder
//!   resolves it to, with nullability, precision, scale and time-attribute kind.
//! - [`casts`] holds Flink's cast rules: `LogicalTypeCasts` (implicit and explicit), the SQL
//!   validator's `CAST`/`TRY_CAST` check and the runtime cast rules (see [`casts::cast_verdict`]).
//! - [`merging`] holds `LogicalTypeMerging`: the common type of a set of types and the decimal
//!   precision and scale rules of arithmetic and aggregates.
//! - [`arrow`] maps logical types onto Arrow; the table is in its module docs.
//!
//! The reference is Apache Flink at `release-2.2.1` (`flink-table-common`, and
//! `flink-table-planner`'s copies of Calcite 1.36.0 classes where Flink's SQL front end uses
//! them).

pub mod arrow;
pub mod casts;
mod logical;
pub mod merging;
mod syntax;

pub use logical::{
    DEFAULT_DAY_PRECISION, DEFAULT_DECIMAL_PRECISION, DEFAULT_FRACTIONAL_PRECISION,
    DEFAULT_TIMESTAMP_PRECISION, DEFAULT_YEAR_PRECISION, DayTimeResolution, LogicalKind,
    LogicalType, LogicalTypeFamily, LogicalTypeRoot, MAX_DECIMAL_PRECISION, MAX_LENGTH,
    RowField as LogicalRowField, TimestampKind, TypeError, YearMonthResolution, escape_identifier,
};
pub use syntax::{FlinkType, RowField, TypeKind, parse_type, type_of_cast};

pub(crate) use syntax::as_data_type;
