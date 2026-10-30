//! Formats: rows to and from bytes, as Flink's format factories do.

pub mod json;

use arrow::datatypes::{DataType, Field};

/// The engine's row-kind column: Flink's `RowKind` as `Int8` (0 `+I`, 1 `-U`, 2 `+U`, 3 `-D`).
pub const ROW_KIND_COLUMN: &str = "$row_kind$";

pub fn row_kind_field() -> Field {
    Field::new(ROW_KIND_COLUMN, DataType::Int8, false)
}

/// Flink's `RowKind`, with the engine's codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum RowKind {
    Insert = 0,
    UpdateBefore = 1,
    UpdateAfter = 2,
    Delete = 3,
}

impl RowKind {
    pub fn from_i8(code: i8) -> RowKind {
        match code {
            1 => RowKind::UpdateBefore,
            2 => RowKind::UpdateAfter,
            3 => RowKind::Delete,
            _ => RowKind::Insert,
        }
    }

    pub fn short(self) -> &'static str {
        match self {
            RowKind::Insert => "+I",
            RowKind::UpdateBefore => "-U",
            RowKind::UpdateAfter => "+U",
            RowKind::Delete => "-D",
        }
    }
}

/// A format error, in Flink's wording where Flink has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatError(pub String);

impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FormatError {}
