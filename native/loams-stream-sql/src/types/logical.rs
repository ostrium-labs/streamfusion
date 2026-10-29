//! Flink's logical types (`org.apache.flink.table.types.logical`, release-2.2.1).
//!
//! A [`LogicalType`] is a [`LogicalKind`] plus nullability, as in Flink, where every type carries
//! its own `isNullable()`. `Display` prints Flink's `asSummaryString()` (what `DESCRIBE`, error
//! messages and `typeof` show: `STRING`, `TIMESTAMP(3) *ROWTIME*`, `DECIMAL(10, 2) NOT NULL`);
//! [`LogicalType::serializable`] prints `asSerializableString()` (what `SHOW CREATE TABLE` shows).

use std::fmt;

use crate::types::syntax::{FlinkType, TypeKind};

/// `CharType.MAX_LENGTH`, `VarCharType.MAX_LENGTH`, `BinaryType.MAX_LENGTH`,
/// `VarBinaryType.MAX_LENGTH`.
pub const MAX_LENGTH: u32 = i32::MAX as u32;
/// `DecimalType.MAX_PRECISION`.
pub const MAX_DECIMAL_PRECISION: u8 = 38;
/// `DecimalType.DEFAULT_PRECISION` (what a bare `DECIMAL` means in Flink SQL).
pub const DEFAULT_DECIMAL_PRECISION: u8 = 10;
/// `TimestampType.DEFAULT_PRECISION` and `LocalZonedTimestampType.DEFAULT_PRECISION`.
pub const DEFAULT_TIMESTAMP_PRECISION: u8 = 6;
/// `DayTimeIntervalType.DEFAULT_DAY_PRECISION`.
pub const DEFAULT_DAY_PRECISION: u8 = 2;
/// `DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION`.
pub const DEFAULT_FRACTIONAL_PRECISION: u8 = 6;
/// `YearMonthIntervalType.DEFAULT_PRECISION`.
pub const DEFAULT_YEAR_PRECISION: u8 = 2;

/// `LogicalTypeRoot`: the type constructor without parameters, in Flink's declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LogicalTypeRoot {
    Char,
    Varchar,
    Boolean,
    Binary,
    Varbinary,
    Decimal,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    Date,
    TimeWithoutTimeZone,
    TimestampWithoutTimeZone,
    TimestampWithTimeZone,
    TimestampWithLocalTimeZone,
    IntervalYearMonth,
    IntervalDayTime,
    Array,
    Multiset,
    Map,
    Row,
    DistinctType,
    StructuredType,
    Null,
    Raw,
    Symbol,
    Unresolved,
    Descriptor,
    Variant,
}

/// `LogicalTypeFamily`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogicalTypeFamily {
    Predefined,
    Constructed,
    UserDefined,
    CharacterString,
    BinaryString,
    Numeric,
    IntegerNumeric,
    ExactNumeric,
    ApproximateNumeric,
    Datetime,
    Time,
    Timestamp,
    Interval,
    Collection,
    Extension,
    Descriptor,
}

impl LogicalTypeRoot {
    /// Every root, in declaration order.
    pub const ALL: [LogicalTypeRoot; 31] = [
        LogicalTypeRoot::Char,
        LogicalTypeRoot::Varchar,
        LogicalTypeRoot::Boolean,
        LogicalTypeRoot::Binary,
        LogicalTypeRoot::Varbinary,
        LogicalTypeRoot::Decimal,
        LogicalTypeRoot::TinyInt,
        LogicalTypeRoot::SmallInt,
        LogicalTypeRoot::Integer,
        LogicalTypeRoot::BigInt,
        LogicalTypeRoot::Float,
        LogicalTypeRoot::Double,
        LogicalTypeRoot::Date,
        LogicalTypeRoot::TimeWithoutTimeZone,
        LogicalTypeRoot::TimestampWithoutTimeZone,
        LogicalTypeRoot::TimestampWithTimeZone,
        LogicalTypeRoot::TimestampWithLocalTimeZone,
        LogicalTypeRoot::IntervalYearMonth,
        LogicalTypeRoot::IntervalDayTime,
        LogicalTypeRoot::Array,
        LogicalTypeRoot::Multiset,
        LogicalTypeRoot::Map,
        LogicalTypeRoot::Row,
        LogicalTypeRoot::DistinctType,
        LogicalTypeRoot::StructuredType,
        LogicalTypeRoot::Null,
        LogicalTypeRoot::Raw,
        LogicalTypeRoot::Symbol,
        LogicalTypeRoot::Unresolved,
        LogicalTypeRoot::Descriptor,
        LogicalTypeRoot::Variant,
    ];

    /// `LogicalTypeRoot.getFamilies()`.
    pub fn families(self) -> &'static [LogicalTypeFamily] {
        use LogicalTypeFamily as F;
        match self {
            LogicalTypeRoot::Char | LogicalTypeRoot::Varchar => {
                &[F::Predefined, F::CharacterString]
            }
            LogicalTypeRoot::Boolean => &[F::Predefined],
            LogicalTypeRoot::Binary | LogicalTypeRoot::Varbinary => {
                &[F::Predefined, F::BinaryString]
            }
            LogicalTypeRoot::Decimal => &[F::Predefined, F::Numeric, F::ExactNumeric],
            LogicalTypeRoot::TinyInt
            | LogicalTypeRoot::SmallInt
            | LogicalTypeRoot::Integer
            | LogicalTypeRoot::BigInt => &[
                F::Predefined,
                F::Numeric,
                F::IntegerNumeric,
                F::ExactNumeric,
            ],
            LogicalTypeRoot::Float | LogicalTypeRoot::Double => {
                &[F::Predefined, F::Numeric, F::ApproximateNumeric]
            }
            LogicalTypeRoot::Date => &[F::Predefined, F::Datetime],
            LogicalTypeRoot::TimeWithoutTimeZone => &[F::Predefined, F::Datetime, F::Time],
            LogicalTypeRoot::TimestampWithoutTimeZone | LogicalTypeRoot::TimestampWithTimeZone => {
                &[F::Predefined, F::Datetime, F::Timestamp]
            }
            LogicalTypeRoot::TimestampWithLocalTimeZone => {
                &[F::Predefined, F::Datetime, F::Timestamp, F::Extension]
            }
            LogicalTypeRoot::IntervalYearMonth | LogicalTypeRoot::IntervalDayTime => {
                &[F::Predefined, F::Interval]
            }
            LogicalTypeRoot::Array | LogicalTypeRoot::Multiset => &[F::Constructed, F::Collection],
            LogicalTypeRoot::Map => &[F::Constructed, F::Extension],
            LogicalTypeRoot::Row => &[F::Constructed],
            LogicalTypeRoot::DistinctType | LogicalTypeRoot::StructuredType => &[F::UserDefined],
            LogicalTypeRoot::Null
            | LogicalTypeRoot::Raw
            | LogicalTypeRoot::Symbol
            | LogicalTypeRoot::Unresolved
            | LogicalTypeRoot::Variant => &[F::Extension],
            LogicalTypeRoot::Descriptor => &[F::Descriptor],
        }
    }

    pub fn is(self, family: LogicalTypeFamily) -> bool {
        self.families().contains(&family)
    }
}

/// `TimestampKind`: a regular timestamp or a time attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampKind {
    Regular,
    Rowtime,
    Proctime,
}

/// `YearMonthIntervalType.YearMonthResolution`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum YearMonthResolution {
    Year,
    YearToMonth,
    Month,
}

/// `DayTimeIntervalType.DayTimeResolution`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DayTimeResolution {
    Day,
    DayToHour,
    DayToMinute,
    DayToSecond,
    Hour,
    HourToMinute,
    HourToSecond,
    Minute,
    MinuteToSecond,
    Second,
}

/// A `ROW` field: name, type and optional description.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowField {
    pub name: String,
    pub ty: LogicalType,
    pub description: Option<String>,
}

impl RowField {
    pub fn new(name: impl Into<String>, ty: LogicalType) -> Self {
        RowField {
            name: name.into(),
            ty,
            description: None,
        }
    }
}

/// A logical type constructor with its parameters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalKind {
    /// `CHAR(n)`; `n == 0` only for the empty string literal (`CharType.ofEmptyLiteral`).
    Char(u32),
    /// `VARCHAR(n)`; `VARCHAR(2147483647)` is `STRING`.
    Varchar(u32),
    Boolean,
    Binary(u32),
    /// `VARBINARY(n)`; `VARBINARY(2147483647)` is `BYTES`.
    Varbinary(u32),
    Decimal {
        precision: u8,
        scale: u8,
    },
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    /// Flink's 4-byte `FLOAT` (SQL `FLOAT` and `REAL`).
    Float,
    Double,
    Date,
    /// `TIME(p)`.
    Time(u8),
    /// `TIMESTAMP(p)`, possibly a rowtime attribute.
    Timestamp {
        precision: u8,
        kind: TimestampKind,
    },
    /// `TIMESTAMP(p) WITH TIME ZONE` (exists in the type system; Flink's planner rejects it).
    TimestampTz(u8),
    /// `TIMESTAMP_LTZ(p)`, possibly a rowtime or proctime attribute.
    TimestampLtz {
        precision: u8,
        kind: TimestampKind,
    },
    IntervalYearMonth {
        resolution: YearMonthResolution,
        year_precision: u8,
    },
    IntervalDayTime {
        resolution: DayTimeResolution,
        day_precision: u8,
        fractional_precision: u8,
    },
    Array(Box<LogicalType>),
    Multiset(Box<LogicalType>),
    Map(Box<LogicalType>, Box<LogicalType>),
    Row(Vec<RowField>),
    /// The type of an untyped `NULL` literal.
    Null,
    /// `RAW('class', 'snapshot')`.
    Raw {
        class: String,
        snapshot: String,
    },
    /// The type of a flag argument (`TRIM(BOTH ...)`, `EXTRACT(DAY ...)`).
    Symbol,
    /// The type of a `DESCRIPTOR(...)` argument.
    Descriptor,
    Variant,
}

/// A Flink logical type: constructor, parameters and nullability.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalType {
    pub kind: LogicalKind,
    pub nullable: bool,
}

impl From<LogicalKind> for LogicalType {
    fn from(kind: LogicalKind) -> Self {
        LogicalType {
            kind,
            nullable: true,
        }
    }
}

/// Constructors for the common types (nullable, as Flink's defaults).
impl LogicalType {
    pub fn new(kind: LogicalKind) -> Self {
        kind.into()
    }
    pub fn string() -> Self {
        LogicalKind::Varchar(MAX_LENGTH).into()
    }
    pub fn varchar(n: u32) -> Self {
        LogicalKind::Varchar(n).into()
    }
    pub fn char(n: u32) -> Self {
        LogicalKind::Char(n).into()
    }
    pub fn bytes() -> Self {
        LogicalKind::Varbinary(MAX_LENGTH).into()
    }
    pub fn boolean() -> Self {
        LogicalKind::Boolean.into()
    }
    pub fn tinyint() -> Self {
        LogicalKind::TinyInt.into()
    }
    pub fn smallint() -> Self {
        LogicalKind::SmallInt.into()
    }
    pub fn int() -> Self {
        LogicalKind::Int.into()
    }
    pub fn bigint() -> Self {
        LogicalKind::BigInt.into()
    }
    pub fn float() -> Self {
        LogicalKind::Float.into()
    }
    pub fn double() -> Self {
        LogicalKind::Double.into()
    }
    pub fn date() -> Self {
        LogicalKind::Date.into()
    }
    pub fn decimal(precision: u8, scale: u8) -> Self {
        LogicalKind::Decimal { precision, scale }.into()
    }
    pub fn time(precision: u8) -> Self {
        LogicalKind::Time(precision).into()
    }
    pub fn timestamp(precision: u8) -> Self {
        LogicalKind::Timestamp {
            precision,
            kind: TimestampKind::Regular,
        }
        .into()
    }
    pub fn timestamp_ltz(precision: u8) -> Self {
        LogicalKind::TimestampLtz {
            precision,
            kind: TimestampKind::Regular,
        }
        .into()
    }
    pub fn rowtime(precision: u8) -> Self {
        LogicalKind::Timestamp {
            precision,
            kind: TimestampKind::Rowtime,
        }
        .into()
    }
    /// `PROCTIME()`: `TIMESTAMP_LTZ(3) *PROCTIME* NOT NULL`.
    pub fn proctime() -> Self {
        LogicalType::new(LogicalKind::TimestampLtz {
            precision: 3,
            kind: TimestampKind::Proctime,
        })
        .not_null()
    }
    pub fn null() -> Self {
        LogicalKind::Null.into()
    }
    pub fn array(element: LogicalType) -> Self {
        LogicalKind::Array(Box::new(element)).into()
    }
    pub fn multiset(element: LogicalType) -> Self {
        LogicalKind::Multiset(Box::new(element)).into()
    }
    pub fn map(key: LogicalType, value: LogicalType) -> Self {
        LogicalKind::Map(Box::new(key), Box::new(value)).into()
    }
    pub fn row(fields: Vec<RowField>) -> Self {
        LogicalKind::Row(fields).into()
    }
    pub fn interval_month() -> Self {
        LogicalKind::IntervalYearMonth {
            resolution: YearMonthResolution::Month,
            year_precision: DEFAULT_YEAR_PRECISION,
        }
        .into()
    }
    /// `INTERVAL SECOND(3)`: the type Flink gives every day-time interval value in a query result.
    pub fn interval_second(fractional_precision: u8) -> Self {
        LogicalKind::IntervalDayTime {
            resolution: DayTimeResolution::Second,
            day_precision: DEFAULT_DAY_PRECISION,
            fractional_precision,
        }
        .into()
    }
}

impl LogicalType {
    /// `copy(nullable)`.
    pub fn with_nullable(&self, nullable: bool) -> Self {
        LogicalType {
            kind: self.kind.clone(),
            nullable,
        }
    }

    pub fn not_null(self) -> Self {
        LogicalType {
            nullable: false,
            ..self
        }
    }

    pub fn nullable(self) -> Self {
        LogicalType {
            nullable: true,
            ..self
        }
    }

    pub fn root(&self) -> LogicalTypeRoot {
        match &self.kind {
            LogicalKind::Char(_) => LogicalTypeRoot::Char,
            LogicalKind::Varchar(_) => LogicalTypeRoot::Varchar,
            LogicalKind::Boolean => LogicalTypeRoot::Boolean,
            LogicalKind::Binary(_) => LogicalTypeRoot::Binary,
            LogicalKind::Varbinary(_) => LogicalTypeRoot::Varbinary,
            LogicalKind::Decimal { .. } => LogicalTypeRoot::Decimal,
            LogicalKind::TinyInt => LogicalTypeRoot::TinyInt,
            LogicalKind::SmallInt => LogicalTypeRoot::SmallInt,
            LogicalKind::Int => LogicalTypeRoot::Integer,
            LogicalKind::BigInt => LogicalTypeRoot::BigInt,
            LogicalKind::Float => LogicalTypeRoot::Float,
            LogicalKind::Double => LogicalTypeRoot::Double,
            LogicalKind::Date => LogicalTypeRoot::Date,
            LogicalKind::Time(_) => LogicalTypeRoot::TimeWithoutTimeZone,
            LogicalKind::Timestamp { .. } => LogicalTypeRoot::TimestampWithoutTimeZone,
            LogicalKind::TimestampTz(_) => LogicalTypeRoot::TimestampWithTimeZone,
            LogicalKind::TimestampLtz { .. } => LogicalTypeRoot::TimestampWithLocalTimeZone,
            LogicalKind::IntervalYearMonth { .. } => LogicalTypeRoot::IntervalYearMonth,
            LogicalKind::IntervalDayTime { .. } => LogicalTypeRoot::IntervalDayTime,
            LogicalKind::Array(_) => LogicalTypeRoot::Array,
            LogicalKind::Multiset(_) => LogicalTypeRoot::Multiset,
            LogicalKind::Map(..) => LogicalTypeRoot::Map,
            LogicalKind::Row(_) => LogicalTypeRoot::Row,
            LogicalKind::Null => LogicalTypeRoot::Null,
            LogicalKind::Raw { .. } => LogicalTypeRoot::Raw,
            LogicalKind::Symbol => LogicalTypeRoot::Symbol,
            LogicalKind::Descriptor => LogicalTypeRoot::Descriptor,
            LogicalKind::Variant => LogicalTypeRoot::Variant,
        }
    }

    /// `is(LogicalTypeFamily)`.
    pub fn is(&self, family: LogicalTypeFamily) -> bool {
        self.root().is(family)
    }

    /// `is(LogicalTypeRoot)`.
    pub fn is_root(&self, root: LogicalTypeRoot) -> bool {
        self.root() == root
    }

    /// `getChildren()`.
    pub fn children(&self) -> Vec<&LogicalType> {
        match &self.kind {
            LogicalKind::Array(e) | LogicalKind::Multiset(e) => vec![e],
            LogicalKind::Map(k, v) => vec![k, v],
            LogicalKind::Row(fields) => fields.iter().map(|f| &f.ty).collect(),
            _ => vec![],
        }
    }

    /// `LogicalTypeChecks.getLength` for character and binary strings.
    pub fn length(&self) -> Option<u32> {
        match self.kind {
            LogicalKind::Char(n)
            | LogicalKind::Varchar(n)
            | LogicalKind::Binary(n)
            | LogicalKind::Varbinary(n) => Some(n),
            _ => None,
        }
    }

    /// `LogicalTypeChecks.getPrecision`: decimal precision, the fixed precision of the integer
    /// types (`TINYINT` 3, `SMALLINT` 5, `INT` 10, `BIGINT` 19), time and timestamp precision.
    pub fn precision(&self) -> Option<u8> {
        match self.kind {
            LogicalKind::Decimal { precision, .. } => Some(precision),
            LogicalKind::TinyInt => Some(3),
            LogicalKind::SmallInt => Some(5),
            LogicalKind::Int => Some(10),
            LogicalKind::BigInt => Some(19),
            LogicalKind::Time(p)
            | LogicalKind::TimestampTz(p)
            | LogicalKind::Timestamp { precision: p, .. }
            | LogicalKind::TimestampLtz { precision: p, .. } => Some(p),
            _ => None,
        }
    }

    /// `LogicalTypeChecks.getScale`: decimal scale, 0 for the integer types.
    pub fn scale(&self) -> Option<u8> {
        match self.kind {
            LogicalKind::Decimal { scale, .. } => Some(scale),
            LogicalKind::TinyInt
            | LogicalKind::SmallInt
            | LogicalKind::Int
            | LogicalKind::BigInt => Some(0),
            _ => None,
        }
    }

    /// The time-attribute kind of a timestamp (`Regular` for every other type).
    pub fn timestamp_kind(&self) -> TimestampKind {
        match self.kind {
            LogicalKind::Timestamp { kind, .. } | LogicalKind::TimestampLtz { kind, .. } => kind,
            _ => TimestampKind::Regular,
        }
    }

    pub fn is_rowtime(&self) -> bool {
        self.timestamp_kind() == TimestampKind::Rowtime
    }

    pub fn is_proctime(&self) -> bool {
        self.timestamp_kind() == TimestampKind::Proctime
    }

    pub fn is_time_attribute(&self) -> bool {
        self.timestamp_kind() != TimestampKind::Regular
    }

    /// The type with time-attribute markers removed (Flink's materialisation of a time indicator:
    /// `TIMESTAMP(3) *ROWTIME*` becomes `TIMESTAMP(3)`), recursively.
    pub fn materialized(&self) -> LogicalType {
        let kind = match &self.kind {
            LogicalKind::Timestamp { precision, .. } => LogicalKind::Timestamp {
                precision: *precision,
                kind: TimestampKind::Regular,
            },
            LogicalKind::TimestampLtz { precision, .. } => LogicalKind::TimestampLtz {
                precision: *precision,
                kind: TimestampKind::Regular,
            },
            LogicalKind::Array(e) => LogicalKind::Array(Box::new(e.materialized())),
            LogicalKind::Multiset(e) => LogicalKind::Multiset(Box::new(e.materialized())),
            LogicalKind::Map(k, v) => {
                LogicalKind::Map(Box::new(k.materialized()), Box::new(v.materialized()))
            }
            LogicalKind::Row(fields) => LogicalKind::Row(
                fields
                    .iter()
                    .map(|f| RowField {
                        name: f.name.clone(),
                        ty: f.ty.materialized(),
                        description: f.description.clone(),
                    })
                    .collect(),
            ),
            other => other.clone(),
        };
        LogicalType {
            kind,
            nullable: self.nullable,
        }
    }

    /// Equality ignoring nullability at the top level (`copy(true).equals(other.copy(true))`).
    pub fn equals_ignoring_nullability(&self, other: &LogicalType) -> bool {
        self.kind == other.kind
    }

    /// `YearMonthIntervalType`/`DayTimeIntervalType` with a single field
    /// (`LogicalTypeChecks.isSingleFieldInterval`).
    pub fn is_single_field_interval(&self) -> bool {
        match self.kind {
            LogicalKind::IntervalYearMonth { resolution, .. } => {
                resolution != YearMonthResolution::YearToMonth
            }
            LogicalKind::IntervalDayTime { resolution, .. } => matches!(
                resolution,
                DayTimeResolution::Day
                    | DayTimeResolution::Hour
                    | DayTimeResolution::Minute
                    | DayTimeResolution::Second
            ),
            _ => false,
        }
    }

    /// `asSerializableString()`: the type as DDL spells it (`SHOW CREATE TABLE`).
    pub fn serializable(&self) -> String {
        let mut out = String::new();
        write_type(&mut out, self, Form::Serializable);
        out
    }

    /// `asSummaryString()` of the nullable copy, as `DESCRIBE` prints the `type` column.
    pub fn describe_string(&self) -> String {
        self.with_nullable(true).to_string()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Form {
    Summary,
    Serializable,
}

/// `EncodingUtils.escapeIdentifier`.
pub fn escape_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn write_type(out: &mut String, ty: &LogicalType, form: Form) {
    use std::fmt::Write;
    let attribute = match (&ty.kind, form) {
        (
            LogicalKind::Timestamp { kind, .. } | LogicalKind::TimestampLtz { kind, .. },
            Form::Summary,
        ) if *kind != TimestampKind::Regular => Some(*kind),
        _ => None,
    };
    match &ty.kind {
        LogicalKind::Char(n) => write!(out, "CHAR({n})").unwrap(),
        LogicalKind::Varchar(n) if *n == MAX_LENGTH && form == Form::Summary => {
            out.push_str("STRING")
        }
        LogicalKind::Varchar(n) => write!(out, "VARCHAR({n})").unwrap(),
        LogicalKind::Boolean => out.push_str("BOOLEAN"),
        LogicalKind::Binary(n) => write!(out, "BINARY({n})").unwrap(),
        LogicalKind::Varbinary(n) if *n == MAX_LENGTH && form == Form::Summary => {
            out.push_str("BYTES")
        }
        LogicalKind::Varbinary(n) => write!(out, "VARBINARY({n})").unwrap(),
        LogicalKind::Decimal { precision, scale } => {
            write!(out, "DECIMAL({precision}, {scale})").unwrap()
        }
        LogicalKind::TinyInt => out.push_str("TINYINT"),
        LogicalKind::SmallInt => out.push_str("SMALLINT"),
        LogicalKind::Int => out.push_str("INT"),
        LogicalKind::BigInt => out.push_str("BIGINT"),
        LogicalKind::Float => out.push_str("FLOAT"),
        LogicalKind::Double => out.push_str("DOUBLE"),
        LogicalKind::Date => out.push_str("DATE"),
        LogicalKind::Time(p) => write!(out, "TIME({p})").unwrap(),
        LogicalKind::Timestamp { precision, .. } => write!(out, "TIMESTAMP({precision})").unwrap(),
        LogicalKind::TimestampTz(p) => write!(out, "TIMESTAMP({p}) WITH TIME ZONE").unwrap(),
        LogicalKind::TimestampLtz { precision, .. } => match form {
            Form::Summary => write!(out, "TIMESTAMP_LTZ({precision})").unwrap(),
            Form::Serializable => {
                write!(out, "TIMESTAMP({precision}) WITH LOCAL TIME ZONE").unwrap()
            }
        },
        LogicalKind::IntervalYearMonth {
            resolution,
            year_precision,
        } => match resolution {
            YearMonthResolution::Year => write!(out, "INTERVAL YEAR({year_precision})").unwrap(),
            YearMonthResolution::YearToMonth => {
                write!(out, "INTERVAL YEAR({year_precision}) TO MONTH").unwrap()
            }
            YearMonthResolution::Month => out.push_str("INTERVAL MONTH"),
        },
        LogicalKind::IntervalDayTime {
            resolution,
            day_precision: d,
            fractional_precision: f,
        } => match resolution {
            DayTimeResolution::Day => write!(out, "INTERVAL DAY({d})").unwrap(),
            DayTimeResolution::DayToHour => write!(out, "INTERVAL DAY({d}) TO HOUR").unwrap(),
            DayTimeResolution::DayToMinute => write!(out, "INTERVAL DAY({d}) TO MINUTE").unwrap(),
            DayTimeResolution::DayToSecond => {
                write!(out, "INTERVAL DAY({d}) TO SECOND({f})").unwrap()
            }
            DayTimeResolution::Hour => out.push_str("INTERVAL HOUR"),
            DayTimeResolution::HourToMinute => out.push_str("INTERVAL HOUR TO MINUTE"),
            DayTimeResolution::HourToSecond => write!(out, "INTERVAL HOUR TO SECOND({f})").unwrap(),
            DayTimeResolution::Minute => out.push_str("INTERVAL MINUTE"),
            DayTimeResolution::MinuteToSecond => {
                write!(out, "INTERVAL MINUTE TO SECOND({f})").unwrap()
            }
            DayTimeResolution::Second => write!(out, "INTERVAL SECOND({f})").unwrap(),
        },
        LogicalKind::Array(e) => {
            out.push_str("ARRAY<");
            write_type(out, e, form);
            out.push('>');
        }
        LogicalKind::Multiset(e) => {
            out.push_str("MULTISET<");
            write_type(out, e, form);
            out.push('>');
        }
        LogicalKind::Map(k, v) => {
            out.push_str("MAP<");
            write_type(out, k, form);
            out.push_str(", ");
            write_type(out, v, form);
            out.push('>');
        }
        LogicalKind::Row(fields) => {
            out.push_str("ROW<");
            for (i, field) in fields.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&escape_identifier(&field.name));
                out.push(' ');
                write_type(out, &field.ty, form);
                if let Some(description) = &field.description {
                    match form {
                        Form::Summary => out.push_str(" '...'"),
                        Form::Serializable => {
                            write!(out, " '{}'", escape_string(description)).unwrap()
                        }
                    }
                }
            }
            out.push('>');
        }
        LogicalKind::Null => out.push_str("NULL"),
        LogicalKind::Raw { class, snapshot } => match form {
            Form::Summary => write!(out, "RAW('{}', '...')", escape_string(class)).unwrap(),
            Form::Serializable => write!(
                out,
                "RAW('{}', '{}')",
                escape_string(class),
                escape_string(snapshot)
            )
            .unwrap(),
        },
        LogicalKind::Symbol => out.push_str("SYMBOL"),
        LogicalKind::Descriptor => out.push_str("DESCRIPTOR"),
        LogicalKind::Variant => out.push_str("VARIANT"),
    }
    // `withNullability`: NullType is always nullable and prints no suffix.
    if !ty.nullable && !matches!(ty.kind, LogicalKind::Null) {
        out.push_str(" NOT NULL");
    }
    if let Some(kind) = attribute {
        out.push_str(match kind {
            TimestampKind::Rowtime => " *ROWTIME*",
            TimestampKind::Proctime => " *PROCTIME*",
            TimestampKind::Regular => "",
        });
    }
}

impl fmt::Display for LogicalType {
    /// `asSummaryString()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::new();
        write_type(&mut out, self, Form::Summary);
        f.write_str(&out)
    }
}

/// A type the binder cannot accept, with Flink's message where Flink has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeError {
    /// Flink's `ValidationException` message for an invalid parameter.
    Invalid(String),
    /// A type Flink has but Loams Stream does not accept yet (`RAW`, structured types), named.
    Unsupported(&'static str),
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeError::Invalid(message) => f.write_str(message),
            TypeError::Unsupported(name) => write!(f, "{name} is not supported yet"),
        }
    }
}

fn check(ok: bool, message: impl FnOnce() -> String) -> Result<(), TypeError> {
    if ok {
        Ok(())
    } else {
        Err(TypeError::Invalid(message()))
    }
}

fn length(n: Option<u64>, default: u32, what: &str) -> Result<u32, TypeError> {
    let n = n.unwrap_or(default as u64);
    check((1..=MAX_LENGTH as u64).contains(&n), || {
        format!("{what} length must be between 1 and {MAX_LENGTH} (both inclusive).")
    })?;
    Ok(n as u32)
}

fn precision(n: Option<u64>, default: u8, max: u8, message: &str) -> Result<u8, TypeError> {
    let n = n.unwrap_or(default as u64);
    check(n <= max as u64, || {
        format!("{message} precision must be between 0 and {max} (both inclusive).")
    })?;
    Ok(n as u8)
}

impl LogicalType {
    /// Resolves a parsed type the way Flink's SQL front end does: `VARCHAR` without a length is
    /// `STRING` (`FlinkTypeSystem`), a bare `DECIMAL` is `DECIMAL(10, 0)` (`FlinkTypeFactory`),
    /// `TIMESTAMP` is `TIMESTAMP(6)`, `TIME` is `TIME(0)`, `CHAR` and `BINARY` are length 1,
    /// `REAL` is `FLOAT`, and an interval qualifier takes Flink's default precisions.
    pub fn from_syntax(ty: &FlinkType) -> Result<LogicalType, TypeError> {
        let kind = match &ty.kind {
            TypeKind::Char(n) => LogicalKind::Char(length(*n, 1, "Character string")?),
            TypeKind::Varchar(n) => {
                LogicalKind::Varchar(length(*n, MAX_LENGTH, "Variable character string")?)
            }
            TypeKind::String => LogicalKind::Varchar(MAX_LENGTH),
            TypeKind::Boolean => LogicalKind::Boolean,
            TypeKind::Binary(n) => LogicalKind::Binary(length(*n, 1, "Binary string")?),
            TypeKind::Varbinary(n) => {
                LogicalKind::Varbinary(length(*n, MAX_LENGTH, "Variable binary string")?)
            }
            TypeKind::Bytes => LogicalKind::Varbinary(MAX_LENGTH),
            TypeKind::Decimal(p, s) => {
                let precision = p.unwrap_or(DEFAULT_DECIMAL_PRECISION as u64);
                let scale = s.unwrap_or(0);
                check(
                    (1..=MAX_DECIMAL_PRECISION as u64).contains(&precision),
                    || {
                        format!(
                            "Decimal precision must be between 1 and {MAX_DECIMAL_PRECISION} (both inclusive)."
                        )
                    },
                )?;
                check(scale <= precision, || {
                    format!(
                        "Decimal scale must be between 0 and the precision {precision} (both inclusive)."
                    )
                })?;
                LogicalKind::Decimal {
                    precision: precision as u8,
                    scale: scale as u8,
                }
            }
            TypeKind::TinyInt => LogicalKind::TinyInt,
            TypeKind::SmallInt => LogicalKind::SmallInt,
            TypeKind::Int => LogicalKind::Int,
            TypeKind::BigInt => LogicalKind::BigInt,
            TypeKind::Float | TypeKind::Real => LogicalKind::Float,
            TypeKind::Double => LogicalKind::Double,
            TypeKind::Date => LogicalKind::Date,
            TypeKind::Time(p) => LogicalKind::Time(precision(*p, 0, 9, "Time")?),
            TypeKind::Timestamp(p) => LogicalKind::Timestamp {
                precision: precision(*p, DEFAULT_TIMESTAMP_PRECISION, 9, "Timestamp")?,
                kind: TimestampKind::Regular,
            },
            TypeKind::TimestampLtz { precision: p, .. } => LogicalKind::TimestampLtz {
                precision: precision(
                    *p,
                    DEFAULT_TIMESTAMP_PRECISION,
                    9,
                    "Timestamp with local time zone",
                )?,
                kind: TimestampKind::Regular,
            },
            TypeKind::TimestampTz(p) => LogicalKind::TimestampTz(precision(
                *p,
                DEFAULT_TIMESTAMP_PRECISION,
                9,
                "Timestamp with time zone",
            )?),
            TypeKind::Interval(qualifier) => parse_interval_qualifier(qualifier)?,
            TypeKind::Array(e) => LogicalKind::Array(Box::new(LogicalType::from_syntax(e)?)),
            TypeKind::Multiset(e) => LogicalKind::Multiset(Box::new(LogicalType::from_syntax(e)?)),
            TypeKind::Map(k, v) => LogicalKind::Map(
                Box::new(LogicalType::from_syntax(k)?),
                Box::new(LogicalType::from_syntax(v)?),
            ),
            TypeKind::Row(fields) => {
                let mut out = Vec::with_capacity(fields.len());
                for field in fields {
                    if out.iter().any(|f: &RowField| f.name == field.name.value) {
                        return Err(TypeError::Invalid(format!(
                            "Field names must be unique. Found duplicates: [{}]",
                            field.name.value
                        )));
                    }
                    out.push(RowField {
                        name: field.name.value.clone(),
                        ty: LogicalType::from_syntax(&field.ty)?,
                        description: field.comment.clone(),
                    });
                }
                LogicalKind::Row(out)
            }
            TypeKind::Raw { class, snapshot } => LogicalKind::Raw {
                class: class.clone(),
                snapshot: snapshot.clone(),
            },
            TypeKind::Structured { .. } => {
                return Err(TypeError::Unsupported("structured types (STRUCTURED<...>)"));
            }
            TypeKind::UserDefined(name) => {
                return Err(TypeError::Invalid(format!(
                    "Unknown datatype name '{name}'"
                )));
            }
        };
        Ok(LogicalType {
            kind,
            nullable: ty.nullable,
        })
    }
}

/// Parses the normalised interval qualifier the parser keeps (`DAY(2) TO SECOND(3)`, `MONTH`).
fn parse_interval_qualifier(text: &str) -> Result<LogicalKind, TypeError> {
    fn unit(part: &str) -> Result<(&str, Option<u8>, Option<u8>), TypeError> {
        let part = part.trim();
        let (name, args) = match part.find('(') {
            Some(i) => (&part[..i], Some(&part[i + 1..part.len() - 1])),
            None => (part, None),
        };
        let mut numbers = args
            .map(|a| {
                a.split(',')
                    .map(|n| n.trim().parse::<u8>())
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|_| TypeError::Invalid(format!("Invalid interval qualifier '{part}'")))?
            .unwrap_or_default()
            .into_iter();
        Ok((name, numbers.next(), numbers.next()))
    }
    let (start, end) = match text.split_once(" TO ") {
        Some((a, b)) => (unit(a)?, Some(unit(b)?)),
        None => (unit(text)?, None),
    };
    let year_month = |resolution, p: Option<u8>| -> Result<LogicalKind, TypeError> {
        let year_precision = p.unwrap_or(DEFAULT_YEAR_PRECISION);
        check((1..=4).contains(&year_precision), || {
            "Year precision of year-month intervals must be between 1 and 4 (both inclusive)."
                .to_string()
        })?;
        Ok(LogicalKind::IntervalYearMonth {
            resolution,
            year_precision,
        })
    };
    let day_time = |resolution, d: Option<u8>, f: Option<u8>| -> Result<LogicalKind, TypeError> {
        let day_precision = d.unwrap_or(DEFAULT_DAY_PRECISION);
        let fractional_precision = f.unwrap_or(DEFAULT_FRACTIONAL_PRECISION);
        check((1..=6).contains(&day_precision), || {
            "Day precision of day-time intervals must be between 1 and 6 (both inclusive)."
                .to_string()
        })?;
        check(fractional_precision <= 9, || {
            "Fractional precision of day-time intervals must be between 0 and 9 (both inclusive)."
                .to_string()
        })?;
        Ok(LogicalKind::IntervalDayTime {
            resolution,
            day_precision,
            fractional_precision,
        })
    };
    use DayTimeResolution as D;
    match (start, end) {
        (("YEAR", p, _), None) => year_month(YearMonthResolution::Year, p),
        (("YEAR", p, _), Some(("MONTH", _, _))) => year_month(YearMonthResolution::YearToMonth, p),
        (("MONTH", _, _), None) => year_month(YearMonthResolution::Month, None),
        (("DAY", p, _), None) => day_time(D::Day, p, None),
        (("DAY", p, _), Some(("HOUR", _, _))) => day_time(D::DayToHour, p, None),
        (("DAY", p, _), Some(("MINUTE", _, _))) => day_time(D::DayToMinute, p, None),
        (("DAY", p, _), Some(("SECOND", f, _))) => day_time(D::DayToSecond, p, f),
        (("HOUR", _, _), None) => day_time(D::Hour, None, None),
        (("HOUR", _, _), Some(("MINUTE", _, _))) => day_time(D::HourToMinute, None, None),
        (("HOUR", _, _), Some(("SECOND", f, _))) => day_time(D::HourToSecond, None, f),
        (("MINUTE", _, _), None) => day_time(D::Minute, None, None),
        (("MINUTE", _, _), Some(("SECOND", f, _))) => day_time(D::MinuteToSecond, None, f),
        // `SECOND(p)` is the leading precision in Calcite and `SECOND(p, f)` adds the fraction;
        // Flink's `INTERVAL SECOND(f)` type keeps only the fraction.
        (("SECOND", p, f), None) => day_time(D::Second, None, f.or(p.filter(|_| f.is_some()))),
        _ => Err(TypeError::Invalid(format!(
            "Unsupported interval qualifier '{text}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(sql: &str) -> LogicalType {
        LogicalType::from_syntax(&crate::parse_type_str(sql).unwrap()).unwrap()
    }

    #[test]
    fn summary_strings_match_flink() {
        let cases = [
            ("STRING", "STRING"),
            ("VARCHAR", "STRING"),
            ("VARCHAR(32)", "VARCHAR(32)"),
            ("CHAR", "CHAR(1)"),
            ("BYTES", "BYTES"),
            ("DECIMAL", "DECIMAL(10, 0)"),
            ("DECIMAL(23, 3) NOT NULL", "DECIMAL(23, 3) NOT NULL"),
            ("TIMESTAMP", "TIMESTAMP(6)"),
            ("TIMESTAMP(3) WITH LOCAL TIME ZONE", "TIMESTAMP_LTZ(3)"),
            ("TIME", "TIME(0)"),
            ("REAL", "FLOAT"),
            ("INTERVAL DAY TO SECOND(3)", "INTERVAL DAY(2) TO SECOND(3)"),
            ("INTERVAL YEAR TO MONTH", "INTERVAL YEAR(2) TO MONTH"),
            ("INTERVAL MONTH", "INTERVAL MONTH"),
            (
                "ROW<a INT NOT NULL, b ARRAY<STRING>>",
                "ROW<`a` INT NOT NULL, `b` ARRAY<STRING>>",
            ),
            ("MAP<STRING, BIGINT>", "MAP<STRING, BIGINT>"),
            ("INT MULTISET", "MULTISET<INT>"),
        ];
        for (sql, expected) in cases {
            assert_eq!(resolve(sql).to_string(), expected, "{sql}");
        }
        assert_eq!(
            LogicalType::rowtime(3).to_string(),
            "TIMESTAMP(3) *ROWTIME*"
        );
        assert_eq!(
            LogicalType::proctime().to_string(),
            "TIMESTAMP_LTZ(3) NOT NULL *PROCTIME*"
        );
        assert_eq!(
            LogicalType::proctime().describe_string(),
            "TIMESTAMP_LTZ(3) *PROCTIME*"
        );
        assert_eq!(
            resolve("TIMESTAMP_LTZ(3)").serializable(),
            "TIMESTAMP(3) WITH LOCAL TIME ZONE"
        );
        assert_eq!(resolve("STRING").serializable(), "VARCHAR(2147483647)");
    }

    #[test]
    fn invalid_parameters_use_flink_messages() {
        let err = |sql| LogicalType::from_syntax(&crate::parse_type_str(sql).unwrap()).unwrap_err();
        assert_eq!(
            err("DECIMAL(39, 2)").to_string(),
            "Decimal precision must be between 1 and 38 (both inclusive)."
        );
        assert_eq!(
            err("DECIMAL(5, 6)").to_string(),
            "Decimal scale must be between 0 and the precision 5 (both inclusive)."
        );
        assert_eq!(
            err("TIMESTAMP(10)").to_string(),
            "Timestamp precision must be between 0 and 9 (both inclusive)."
        );
    }
}
