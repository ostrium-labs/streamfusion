//! Flink's cast rules, in three layers.
//!
//! 1. [`supports_implicit_cast`], [`supports_explicit_cast`] and [`supports_avoiding_cast`]: a
//!    port of `LogicalTypeCasts` (`flink-table-common`, release-2.2.1). Implicit casts widen and
//!    generalise types without loss (function arguments, `INSERT` into a sink, set operations);
//!    explicit casts are the logical `CAST` specification.
//! 2. [`sql_cast_allowed`]: the check Flink's SQL validator applies to `CAST(x AS t)` and
//!    `TRY_CAST(x AS t)`. Flink 2.2.1 ships its own copy of Calcite's `SqlCastFunction`, whose
//!    `canCastFrom` uses `LogicalTypeCasts.supportsExplicitCast` only when the source is a
//!    constructed type (`ARRAY`, `MAP`, `MULTISET`, `ROW`, structured, `RAW`, `VARIANT`); for every
//!    other source it uses Calcite 1.36.0's `SqlTypeUtil.canCastFrom` with
//!    `FlinkSqlTypeMappingRule` (Calcite's lenient coercion rule plus Flink's numeric additions).
//!    The two layers differ: for example `CAST(INTERVAL '1' DAY AS DECIMAL)` passes
//!    `LogicalTypeCasts` but not the validator.
//! 3. [`runtime_cast_exists`]: whether `CastRuleProvider` (`flink-table-planner`'s
//!    `functions.casting`) has a rule that generates code for the pair. A cast the validator
//!    accepts but no rule implements fails when the job is translated.
//!
//! [`cast_verdict`] combines them into what a user sees: `implicit`, `explicit` or `refused`. The
//! binder uses it for `CAST` and `TRY_CAST`, and [`supports_implicit_cast`] for implicit coercion.

use super::logical::{LogicalKind, LogicalType, LogicalTypeFamily, LogicalTypeRoot, MAX_LENGTH};

use LogicalTypeFamily as F;
use LogicalTypeRoot as R;

// ---------------------------------------------------------------------------------------------
// Layer 1: LogicalTypeCasts
// ---------------------------------------------------------------------------------------------

/// The roots implicitly castable to `target` (`implicitCastingRules`), besides identity.
fn implicit_rule(target: R, source: R) -> bool {
    if target == source {
        return true;
    }
    match target {
        R::Char => false,
        R::Varchar => source.is(F::CharacterString),
        R::Boolean => false,
        R::Binary => false,
        R::Varbinary => source.is(F::BinaryString),
        R::Decimal => source.is(F::Numeric),
        R::TinyInt => false,
        R::SmallInt => source == R::TinyInt,
        R::Integer => matches!(source, R::TinyInt | R::SmallInt),
        R::BigInt => matches!(source, R::TinyInt | R::SmallInt | R::Integer),
        R::Float => matches!(
            source,
            R::TinyInt | R::SmallInt | R::Integer | R::BigInt | R::Decimal
        ),
        R::Double => source.is(F::Numeric),
        R::Date => source == R::TimestampWithoutTimeZone,
        R::TimeWithoutTimeZone => source == R::TimestampWithoutTimeZone,
        R::TimestampWithoutTimeZone => source == R::TimestampWithLocalTimeZone,
        R::TimestampWithLocalTimeZone => source == R::TimestampWithoutTimeZone,
        _ => false,
    }
}

/// The roots explicitly castable to `target` (`explicitCastingRules`).
fn explicit_rule(target: R, source: R) -> bool {
    let timestamps = matches!(
        source,
        R::TimestampWithoutTimeZone | R::TimestampWithLocalTimeZone
    );
    match target {
        R::Char | R::Varchar => {
            source.is(F::Predefined)
                || source.is(F::Constructed)
                || matches!(source, R::Raw | R::Null | R::StructuredType)
        }
        R::Boolean => source.is(F::CharacterString) || source.is(F::IntegerNumeric),
        R::Binary => source.is(F::CharacterString) || matches!(source, R::Varbinary | R::Raw),
        R::Varbinary => source.is(F::CharacterString) || matches!(source, R::Binary | R::Raw),
        R::Decimal => {
            source.is(F::CharacterString)
                || source.is(F::Interval)
                || source == R::Boolean
                || timestamps
        }
        R::TinyInt | R::SmallInt | R::Integer | R::BigInt => {
            source.is(F::Numeric)
                || source.is(F::CharacterString)
                || source.is(F::Interval)
                || source == R::Boolean
                || timestamps
        }
        R::Float => {
            source.is(F::Numeric)
                || source.is(F::CharacterString)
                || source == R::Boolean
                || timestamps
        }
        R::Double => source.is(F::CharacterString) || source == R::Boolean || timestamps,
        R::Date => source.is(F::Timestamp) || source.is(F::CharacterString),
        R::TimeWithoutTimeZone => {
            source.is(F::Time) || source.is(F::Timestamp) || source.is(F::CharacterString)
        }
        R::TimestampWithoutTimeZone | R::TimestampWithLocalTimeZone => {
            source.is(F::Datetime) || source.is(F::CharacterString) || source.is(F::Numeric)
        }
        R::TimestampWithTimeZone => source.is(F::Datetime) || source.is(F::CharacterString),
        R::IntervalYearMonth | R::IntervalDayTime => {
            source.is(F::ExactNumeric) || source.is(F::CharacterString)
        }
        _ => false,
    }
}

/// `LogicalTypeCasts.supportsImplicitCast`.
pub fn supports_implicit_cast(source: &LogicalType, target: &LogicalType) -> bool {
    supports_casting(source, target, false)
}

/// `LogicalTypeCasts.supportsExplicitCast`.
pub fn supports_explicit_cast(source: &LogicalType, target: &LogicalType) -> bool {
    supports_casting(source, target, true)
}

fn supports_casting(source: &LogicalType, target: &LogicalType, allow_explicit: bool) -> bool {
    // A NOT NULL type cannot store a NULL type, unless the user casts explicitly.
    if source.nullable && !target.nullable && !allow_explicit {
        return false;
    }
    if source.equals_ignoring_nullability(target) {
        return true;
    }
    let (s, t) = (source.root(), target.root());
    if s == R::Null {
        return true;
    }
    if source.is(F::Interval) && target.is(F::ExactNumeric) {
        return source.is_single_field_interval();
    }
    if source.is(F::ExactNumeric) && target.is(F::Interval) {
        return target.is_single_field_interval();
    }
    if source.is(F::Constructed) && target.is(F::Constructed) {
        return supports_constructed_casting(source, target, allow_explicit);
    }
    if (s == R::Raw && !target.is(F::BinaryString) && !target.is(F::CharacterString)) || t == R::Raw
    {
        return false;
    }
    if s == R::Symbol || t == R::Symbol {
        return false;
    }
    if implicit_rule(t, s) {
        return true;
    }
    allow_explicit && explicit_rule(t, s)
}

fn supports_constructed_casting(
    source: &LogicalType,
    target: &LogicalType,
    allow_explicit: bool,
) -> bool {
    if source.root() != target.root() {
        return false;
    }
    let (sc, tc) = (source.children(), target.children());
    sc.len() == tc.len()
        && sc
            .iter()
            .zip(&tc)
            .all(|(s, t)| supports_casting(s, t, allow_explicit))
}

/// `LogicalTypeCasts.supportsAvoidingCast`: the source can be read as the target without a cast
/// (`CHAR(2)` into `VARCHAR(3)`, `NOT NULL` into nullable, same constructed shape with avoidable
/// children; field names do not matter).
pub fn supports_avoiding_cast(source: &LogicalType, target: &LogicalType) -> bool {
    if source.nullable && !target.nullable {
        return false;
    }
    match (&source.kind, &target.kind) {
        (LogicalKind::Char(n) | LogicalKind::Varchar(n), LogicalKind::Varchar(m))
        | (LogicalKind::Binary(n) | LogicalKind::Varbinary(n), LogicalKind::Varbinary(m))
            if n <= m =>
        {
            return true;
        }
        _ => {}
    }
    if source.root() != target.root() {
        return false;
    }
    let (sc, tc) = (source.children(), target.children());
    if sc.is_empty() && tc.is_empty() {
        return source.kind == target.kind;
    }
    sc.len() == tc.len()
        && sc
            .iter()
            .zip(&tc)
            .all(|(s, t)| supports_avoiding_cast(s, t))
}

// ---------------------------------------------------------------------------------------------
// Layer 2: the SQL validator (Flink's SqlCastFunction + Calcite 1.36.0 canCastFrom)
// ---------------------------------------------------------------------------------------------

/// Calcite's `SqlTypeName` for a Flink type, as `FlinkTypeFactory.createFieldTypeFromLogicalType`
/// maps it. Interval types collapse to their Calcite group (all year-month names behave alike in
/// the mapping rule, as do all day-time names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlTypeName {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Float,
    Double,
    Char,
    Varchar,
    Binary,
    Varbinary,
    Date,
    Time,
    Timestamp,
    TimestampLtz,
    IntervalYearMonth,
    IntervalDayTime,
    Null,
    Symbol,
    Array,
    Multiset,
    Map,
    Row,
    /// `COLUMN_LIST` (descriptor).
    ColumnList,
    Variant,
    /// `OTHER`: `RAW`.
    Other,
    /// A type Flink's planner does not map (`TIMESTAMP WITH TIME ZONE`).
    Unmapped,
}

fn sql_type_name(ty: &LogicalType) -> SqlTypeName {
    use SqlTypeName as S;
    match ty.root() {
        R::Char => S::Char,
        R::Varchar => S::Varchar,
        R::Boolean => S::Boolean,
        R::Binary => S::Binary,
        R::Varbinary => S::Varbinary,
        R::Decimal => S::Decimal,
        R::TinyInt => S::TinyInt,
        R::SmallInt => S::SmallInt,
        R::Integer => S::Integer,
        R::BigInt => S::BigInt,
        R::Float => S::Float,
        R::Double => S::Double,
        R::Date => S::Date,
        R::TimeWithoutTimeZone => S::Time,
        R::TimestampWithoutTimeZone => S::Timestamp,
        R::TimestampWithLocalTimeZone => S::TimestampLtz,
        R::IntervalYearMonth => S::IntervalYearMonth,
        R::IntervalDayTime => S::IntervalDayTime,
        R::Null => S::Null,
        R::Symbol => S::Symbol,
        R::Array => S::Array,
        R::Multiset => S::Multiset,
        R::Map => S::Map,
        R::Row | R::StructuredType => S::Row,
        R::Descriptor => S::ColumnList,
        R::Variant => S::Variant,
        R::Raw => S::Other,
        R::TimestampWithTimeZone | R::DistinctType | R::Unresolved => S::Unmapped,
    }
}

/// `FlinkSqlTypeMappingRule.canApplyFrom(to, from)`, as the effective map after Calcite's
/// `SqlTypeAssignmentRule`, `SqlTypeCoercionRule`, its lenient variant and Flink's additions.
/// `None` where the map has no entry for `to` (Calcite raises an `AssertionError`).
fn flink_mapping_rule(to: SqlTypeName, from: SqlTypeName) -> Option<bool> {
    use SqlTypeName as S;
    if to == S::Null {
        return Some(false);
    }
    if from == S::Null {
        return Some(true);
    }
    let numeric = matches!(
        from,
        S::TinyInt | S::SmallInt | S::Integer | S::BigInt | S::Decimal | S::Float | S::Double
    );
    let char_ = matches!(from, S::Char | S::Varchar);
    let binary = matches!(from, S::Binary | S::Varbinary);
    let interval = matches!(from, S::IntervalYearMonth | S::IntervalDayTime);
    let timestamps = matches!(from, S::Timestamp | S::TimestampLtz);
    Some(match to {
        // The lenient rule (and Flink's copy for FLOAT, DOUBLE, DECIMAL).
        S::TinyInt | S::SmallInt | S::Integer | S::BigInt | S::Float | S::Double | S::Decimal => {
            numeric || char_ || from == S::Boolean || timestamps
        }
        S::Char | S::Varchar => {
            numeric
                || char_
                || binary
                || interval
                || timestamps
                || matches!(from, S::Boolean | S::Date | S::Time)
        }
        S::Boolean => from == S::Boolean || char_ || numeric,
        S::Binary | S::Varbinary => binary || char_,
        S::Date => from == S::Date || timestamps || char_ || binary,
        S::Time => from == S::Time || timestamps || char_ || binary,
        S::Timestamp | S::TimestampLtz => {
            timestamps || matches!(from, S::Date | S::Time) || char_ || binary || numeric
        }
        S::IntervalYearMonth => {
            from == S::IntervalYearMonth
                || char_
                || matches!(
                    from,
                    S::TinyInt | S::SmallInt | S::Integer | S::BigInt | S::Decimal
                )
        }
        S::IntervalDayTime => {
            from == S::IntervalDayTime
                || char_
                || matches!(
                    from,
                    S::TinyInt | S::SmallInt | S::Integer | S::BigInt | S::Decimal
                )
        }
        S::Multiset => from == S::Multiset,
        S::Array => from == S::Array || char_,
        S::Map => from == S::Map,
        S::Symbol => from == S::Symbol,
        S::Row | S::ColumnList | S::Variant | S::Other | S::Unmapped | S::Null => return None,
    })
}

/// Calcite 1.36.0 `SqlTypeUtil.canCastFrom(toType, fromType, FlinkSqlTypeMappingRule)`.
fn calcite_can_cast_from(to: &LogicalType, from: &LogicalType) -> bool {
    use SqlTypeName as S;
    if to == from {
        return true;
    }
    let (tn, fname) = (sql_type_name(to), sql_type_name(from));
    if tn == S::Row || fname == S::Row {
        if tn == S::Row {
            if fname != S::Row {
                return fname == S::Null;
            }
            let (tc, fc) = (to.children(), from.children());
            return tc.len() == fc.len()
                && tc.iter().zip(&fc).all(|(t, f)| calcite_can_cast_from(t, f));
        }
        // fromType is a struct and toType is not: same family only (never for ROW).
        return false;
    }
    let component = |ty: &LogicalType| match &ty.kind {
        LogicalKind::Array(e) | LogicalKind::Multiset(e) => Some((**e).clone()),
        _ => None,
    };
    if let (Some(c1), Some(c2)) = (component(to), component(from)) {
        return calcite_can_cast_from(&c1, &c2);
    }
    let interval_exact = (from.is(F::Interval) && to.is(F::ExactNumeric))
        || (to.is(F::Interval) && from.is(F::ExactNumeric));
    if interval_exact {
        let interval = if from.is(F::Interval) { from } else { to };
        if !interval.is_single_field_interval() {
            return false;
        }
    }
    if tn == S::Unmapped || fname == S::Unmapped {
        return false;
    }
    flink_mapping_rule(tn, fname).unwrap_or(false)
}

/// Whether Flink's SQL validator accepts `CAST(x AS target)` (and `TRY_CAST`) for an `x` of type
/// `source` (Flink's copy of `SqlCastFunction.checkOperandTypes`/`canCastFrom`). An untyped `NULL`
/// literal is always accepted.
pub fn sql_cast_allowed(source: &LogicalType, target: &LogicalType) -> bool {
    if target.root() == R::Variant {
        return false;
    }
    if source.root() == R::Null {
        return true;
    }
    match source.root() {
        R::Array | R::Map | R::Multiset | R::StructuredType | R::Row | R::Variant | R::Raw => {
            supports_explicit_cast(source, target)
        }
        _ => calcite_can_cast_from(target, source),
    }
}

// ---------------------------------------------------------------------------------------------
// Layer 3: the runtime cast rules (CastRuleProvider)
// ---------------------------------------------------------------------------------------------

/// `CastRuleProvider.exists`: some cast rule generates code for `source` to `target`. A
/// `CHAR(n)`/`VARCHAR(n)` target resolves through `CharVarCharTrimPadCastRule`, which delegates to
/// the rule for `STRING`, so character targets are looked up as `STRING`.
pub fn runtime_cast_exists(source: &LogicalType, target: &LogicalType) -> bool {
    let source = &source.with_nullable(true);
    let target = &target.with_nullable(true);
    if is_identity_cast(source, target) {
        return true;
    }
    if target.is(F::CharacterString) && !matches!(target.kind, LogicalKind::Varchar(MAX_LENGTH)) {
        return runtime_cast_exists(source, &LogicalType::string());
    }
    let (s, t) = (source.root(), target.root());
    let to_string = matches!(target.kind, LogicalKind::Varchar(MAX_LENGTH));
    let ts = |r: R| {
        matches!(
            r,
            R::TimestampWithoutTimeZone | R::TimestampWithLocalTimeZone
        )
    };
    let int_or_approx = |r: R| r.is(F::IntegerNumeric) || r.is(F::ApproximateNumeric);
    let direct = (s == R::Decimal && t == R::Decimal)
        || (int_or_approx(s) && t == R::Decimal)
        || (s == R::Decimal && int_or_approx(t))
        || (s == R::Boolean && t.is(F::Numeric))
        || (s.is(F::Numeric) && t == R::Boolean)
        || (to_string
            && (s.is(F::Numeric)
                || s == R::Boolean
                || s.is(F::Timestamp)
                || s == R::TimeWithoutTimeZone
                || s == R::Date
                || s.is(F::Interval)
                || s == R::Variant))
        || (s.is(F::BinaryString) && t.is(F::CharacterString))
        || (s == R::Raw && t.is(F::CharacterString))
        || (s == R::Null && t.is(F::CharacterString))
        || (s.is(F::CharacterString)
            && (t == R::Boolean
                || t == R::Decimal
                || int_or_approx(t)
                || t == R::Date
                || t == R::TimeWithoutTimeZone
                || t.is(F::Timestamp)
                || t.is(F::BinaryString)))
        || (ts(s) && (ts(t) || t == R::Date || t == R::TimeWithoutTimeZone || t.is(F::Numeric)))
        || ((s == R::Date || s == R::TimeWithoutTimeZone) && ts(t))
        || (s.is(F::Numeric) && ts(t))
        || (s.is(F::BinaryString) && t.is(F::BinaryString))
        || (s == R::Raw && t.is(F::BinaryString));
    if direct {
        return true;
    }
    // Rules with custom predicates, in registration order.
    numeric_primitive_cast(s, t)
        || match (&source.kind, &target.kind) {
            (LogicalKind::Array(e), _) if t.is(F::CharacterString) => {
                runtime_cast_exists(e, target)
            }
            (LogicalKind::Map(k, v), _) if t.is(F::CharacterString) => {
                runtime_cast_exists(k, target) && runtime_cast_exists(v, target)
            }
            (LogicalKind::Multiset(e), _) if t.is(F::CharacterString) => {
                runtime_cast_exists(e, target)
            }
            (LogicalKind::Row(fields), _) if t.is(F::CharacterString) => {
                fields.iter().all(|f| runtime_cast_exists(&f.ty, target))
            }
            (LogicalKind::Array(a), LogicalKind::Array(b)) => runtime_cast_exists(a, b),
            (LogicalKind::Map(k1, v1), LogicalKind::Map(k2, v2)) => {
                runtime_cast_exists(k1, k2) && runtime_cast_exists(v1, v2)
            }
            (LogicalKind::Multiset(a), LogicalKind::Multiset(b)) => runtime_cast_exists(a, b),
            (LogicalKind::Row(a), LogicalKind::Row(b)) => {
                a.len() >= b.len()
                    && a.iter()
                        .zip(b)
                        .all(|(x, y)| runtime_cast_exists(&x.ty, &y.ty))
            }
            _ => false,
        }
}

/// `IdentityCastRule.isIdentityCast`.
fn is_identity_cast(source: &LogicalType, target: &LogicalType) -> bool {
    let (s, t) = (source.root(), target.root());
    (s == R::IntervalYearMonth && t == R::Integer)
        || (s == R::Integer && t == R::IntervalYearMonth)
        || (s == R::IntervalDayTime && t == R::BigInt)
        || (s == R::BigInt && t == R::IntervalDayTime)
        || supports_avoiding_cast(source, target)
}

/// `NumericPrimitiveCastRule.matches`.
fn numeric_primitive_cast(s: R, t: R) -> bool {
    if s == t {
        return false;
    }
    let prim = |r: R| r.is(F::IntegerNumeric) || r.is(F::ApproximateNumeric);
    (prim(s) && prim(t))
        || (s == R::IntervalYearMonth && t == R::BigInt)
        || (s == R::BigInt && t == R::IntervalYearMonth)
        || (s == R::IntervalDayTime && t == R::Integer)
        || (s == R::Integer && t == R::IntervalDayTime)
        || (matches!(s, R::TinyInt | R::SmallInt) && t == R::IntervalDayTime)
        || (matches!(s, R::TinyInt | R::SmallInt) && t == R::IntervalYearMonth)
}

// ---------------------------------------------------------------------------------------------
// The verdict
// ---------------------------------------------------------------------------------------------

/// What Flink does with a cast from one type to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CastVerdict {
    /// Applied implicitly where a type must widen (`LogicalTypeCasts.supportsImplicitCast`); also
    /// allowed as an explicit `CAST`.
    Implicit,
    /// Only as an explicit `CAST`/`TRY_CAST`: the validator accepts it and a runtime rule exists.
    Explicit,
    /// Rejected (by the validator, or at translation because no runtime rule exists).
    Refused,
}

impl CastVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            CastVerdict::Implicit => "implicit",
            CastVerdict::Explicit => "explicit",
            CastVerdict::Refused => "refused",
        }
    }
}

/// The verdict for a cast between two nullable types: implicit when `LogicalTypeCasts` allows it
/// implicitly and Flink can run it; explicit when the SQL validator accepts `CAST` and a runtime
/// rule exists; refused otherwise.
pub fn cast_verdict(source: &LogicalType, target: &LogicalType) -> CastVerdict {
    let runs = source.root() == R::Null || runtime_cast_exists(source, target);
    let validates = sql_cast_allowed(source, target);
    if supports_implicit_cast(source, target) && runs && validates {
        CastVerdict::Implicit
    } else if validates && runs {
        CastVerdict::Explicit
    } else {
        CastVerdict::Refused
    }
}

/// Whether `CAST(x AS target)` binds for an `x` of type `source`.
pub fn cast_allowed(source: &LogicalType, target: &LogicalType) -> bool {
    cast_verdict(source, target) != CastVerdict::Refused
}

#[cfg(test)]
mod tests {
    //! Cases from Flink 2.2.1's `LogicalTypeCastsTest` (flink-table-common), transcribed.
    use super::*;
    use crate::types::logical::{DayTimeResolution, LogicalKind, YearMonthResolution};

    fn ty(sql: &str) -> LogicalType {
        LogicalType::from_syntax(&crate::parse_type_str(sql).unwrap()).unwrap()
    }

    #[test]
    fn logical_type_casts_test_cases() {
        // `LogicalTypeCastsTest.testData()` at release-2.2.1, without the structured-type rows
        // (structured types are not accepted yet): (source, target, implicit, explicit).
        let cases: &[(&str, &str, bool, bool)] = &[
            ("SMALLINT", "BIGINT", true, true),
            // nullability does not match
            ("SMALLINT NOT NULL", "SMALLINT", true, true),
            ("SMALLINT", "SMALLINT NOT NULL", false, true),
            ("INTERVAL YEAR", "SMALLINT", true, true),
            // not an interval with single field
            ("INTERVAL YEAR TO MONTH", "SMALLINT", false, false),
            ("INT", "DECIMAL(5, 5)", true, true),
            // loss of precision
            ("FLOAT", "INT", false, true),
            ("STRING", "FLOAT", false, true),
            ("FLOAT", "STRING", false, true),
            ("DECIMAL(3, 2)", "STRING", false, true),
            (
                "RAW('java.lang.Integer', 'a')",
                "RAW('java.lang.Integer', 'a')",
                true,
                true,
            ),
            (
                "RAW('java.lang.Integer', 'a')",
                "RAW('java.lang.Object', 'b')",
                false,
                false,
            ),
            ("ARRAY<INT>", "ARRAY<BIGINT>", true, true),
            ("ARRAY<INT>", "ARRAY<STRING>", false, true),
            ("ROW<f1 INT, f2 INT>", "ROW<f1 INT, f2 BIGINT>", true, true),
            (
                "ROW<f1 INT 'description', f2 INT>",
                "ROW<f1 INT, f2 BIGINT>",
                true,
                true,
            ),
            (
                "ROW<f1 INT, f2 INT>",
                "ROW<f1 INT, f2 BOOLEAN>",
                false,
                true,
            ),
            ("ROW<f1 INT, f2 INT>", "STRING", false, true),
            // timestamp type and timestamp_ltz type
            ("TIMESTAMP(9)", "TIMESTAMP(9)", true, true),
            ("TIMESTAMP_LTZ(9)", "TIMESTAMP_LTZ(9)", true, true),
            ("TIMESTAMP(3)", "TIMESTAMP_LTZ(3)", true, true),
            ("TIMESTAMP_LTZ(3)", "TIMESTAMP(3)", true, true),
            ("TIMESTAMP(3)", "TIMESTAMP_LTZ(6)", true, true),
            ("TIMESTAMP_LTZ(3)", "TIMESTAMP(6)", true, true),
            ("TIMESTAMP(3) NOT NULL", "TIMESTAMP_LTZ(6)", true, true),
            ("TIMESTAMP_LTZ(3) NOT NULL", "TIMESTAMP(6)", true, true),
            ("TIMESTAMP(6)", "TIMESTAMP_LTZ(3)", true, true),
            ("TIMESTAMP_LTZ(6)", "TIMESTAMP(3)", true, true),
            // raw to binary and string
            ("RAW('java.lang.Integer', 'a')", "BINARY", false, true),
            ("RAW('java.lang.Integer', 'a')", "STRING", false, true),
        ];
        for (s, t, implicit, explicit) in cases {
            let (s_ty, t_ty) = (ty(s), ty(t));
            assert_eq!(
                supports_implicit_cast(&s_ty, &t_ty),
                *implicit,
                "implicit {s} -> {t}"
            );
            assert_eq!(
                supports_explicit_cast(&s_ty, &t_ty),
                *explicit,
                "explicit {s} -> {t}"
            );
        }
        // NULL to INT and to ROW<f1 INT, f2 INT>: implicit and explicit.
        let null = LogicalType::null();
        for target in ["INT", "ROW<f1 INT, f2 INT>"] {
            assert!(supports_implicit_cast(&null, &ty(target)), "{target}");
            assert!(supports_explicit_cast(&null, &ty(target)), "{target}");
        }
    }

    #[test]
    fn avoiding_casts() {
        assert!(supports_avoiding_cast(&ty("CHAR(2)"), &ty("VARCHAR(3)")));
        assert!(!supports_avoiding_cast(&ty("VARCHAR(2)"), &ty("CHAR(3)")));
        assert!(supports_avoiding_cast(&ty("INT NOT NULL"), &ty("INT")));
        assert!(!supports_avoiding_cast(&ty("INT"), &ty("INT NOT NULL")));
        assert!(supports_avoiding_cast(
            &ty("ROW<a INT, b STRING>"),
            &ty("ROW<x INT, y STRING>")
        ));
        assert!(!supports_avoiding_cast(&ty("INT"), &ty("BIGINT")));
        assert!(supports_avoiding_cast(
            &ty("ARRAY<INT NOT NULL>"),
            &ty("ARRAY<INT>")
        ));
    }

    #[test]
    fn the_validator_differs_from_logical_type_casts() {
        let day = LogicalType::new(LogicalKind::IntervalDayTime {
            resolution: DayTimeResolution::Day,
            day_precision: 2,
            fractional_precision: 6,
        });
        let month = LogicalType::new(LogicalKind::IntervalYearMonth {
            resolution: YearMonthResolution::Month,
            year_precision: 2,
        });
        // LogicalTypeCasts allows INTERVAL -> DECIMAL; FlinkSqlTypeMappingRule does not.
        assert!(supports_explicit_cast(&day, &ty("DECIMAL(10, 0)")));
        assert!(!sql_cast_allowed(&day, &ty("DECIMAL(10, 0)")));
        // INTERVAL -> BIGINT: LogicalTypeCasts yes, the validator no (lenient rule).
        assert!(supports_explicit_cast(&month, &ty("BIGINT")));
        assert!(!sql_cast_allowed(&month, &ty("BIGINT")));
        // BOOLEAN -> DOUBLE: Flink's mapping rule adds it.
        assert!(sql_cast_allowed(&ty("BOOLEAN"), &ty("DOUBLE")));
        // Runtime rules exist for the common casts.
        assert!(runtime_cast_exists(&ty("INT"), &ty("STRING")));
        assert!(runtime_cast_exists(&ty("ARRAY<INT>"), &ty("VARCHAR(10)")));
        assert!(!runtime_cast_exists(&ty("DATE"), &ty("INT")));
    }
}
