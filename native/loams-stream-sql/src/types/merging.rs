//! `LogicalTypeMerging` (flink-table-common, release-2.2.1): the common type of a set of types and
//! the decimal precision and scale rules Flink's planner uses for arithmetic
//! (`FlinkTypeSystem.deriveDecimal*Type`) and aggregates (`deriveSumType`, `deriveAvgAggType`).
//!
//! | Operation | Result precision | Result scale |
//! |---|---|---|
//! | `e1 + e2`, `e1 - e2` | `max(s1, s2) + max(p1 - s1, p2 - s2) + 1` | `max(s1, s2)` |
//! | `e1 * e2` | `p1 + p2 + 1` | `s1 + s2` |
//! | `e1 / e2` | `p1 - s1 + s2 + max(6, s1 + p2 + 1)` | `max(6, s1 + p2 + 1)` |
//! | `e1 % e2` | `min(p1 - s1, p2 - s2) + max(s1, s2)` | `max(s1, s2)` |
//!
//! A precision above 38 is brought back to 38 the Hive/Spark way: the integral digits are kept and
//! the scale gives way, down to `min(scale, 6)`.

use super::logical::{
    DayTimeResolution, LogicalKind, LogicalType, LogicalTypeFamily, LogicalTypeRoot,
    MAX_DECIMAL_PRECISION, RowField, TimestampKind, YearMonthResolution,
};

use LogicalTypeFamily as F;
use LogicalTypeRoot as R;

const MINIMUM_ADJUSTED_SCALE: i32 = 6;

/// A decimal type's precision and scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    pub precision: u8,
    pub scale: u8,
}

impl Decimal {
    pub fn new(precision: u8, scale: u8) -> Self {
        Decimal { precision, scale }
    }

    /// The `NOT NULL` logical type (`new DecimalType(false, p, s)`).
    pub fn to_type(self) -> LogicalType {
        LogicalType::decimal(self.precision, self.scale).not_null()
    }
}

/// `adjustPrecisionScale`.
fn adjust_precision_scale(precision: i32, scale: i32) -> Decimal {
    let max = MAX_DECIMAL_PRECISION as i32;
    if precision <= max {
        Decimal::new(precision as u8, scale as u8)
    } else {
        let digit_part = precision - scale;
        let min_scale_part = scale.min(MINIMUM_ADJUSTED_SCALE);
        let adjust_scale = (max - digit_part).max(min_scale_part);
        Decimal::new(max as u8, adjust_scale as u8)
    }
}

/// `findDivisionDecimalType`.
pub fn division_decimal(p1: u8, s1: u8, p2: u8, s2: u8) -> Decimal {
    let (p1, s1, p2, s2) = (p1 as i32, s1 as i32, p2 as i32, s2 as i32);
    let scale = 6.max(s1 + p2 + 1);
    let precision = p1 - s1 + s2 + scale;
    adjust_precision_scale(precision, scale)
}

/// `findModuloDecimalType`.
pub fn modulo_decimal(p1: u8, s1: u8, p2: u8, s2: u8) -> Decimal {
    let (p1, s1, p2, s2) = (p1 as i32, s1 as i32, p2 as i32, s2 as i32);
    let scale = s1.max(s2);
    let precision = (p1 - s1).min(p2 - s2) + scale;
    adjust_precision_scale(precision, scale)
}

/// `findMultiplicationDecimalType`.
pub fn multiplication_decimal(p1: u8, s1: u8, p2: u8, s2: u8) -> Decimal {
    let (p1, s1, p2, s2) = (p1 as i32, s1 as i32, p2 as i32, s2 as i32);
    adjust_precision_scale(p1 + p2 + 1, s1 + s2)
}

/// `findAdditionDecimalType` (also subtraction).
pub fn addition_decimal(p1: u8, s1: u8, p2: u8, s2: u8) -> Decimal {
    let (p1, s1, p2, s2) = (p1 as i32, s1 as i32, p2 as i32, s2 as i32);
    let scale = s1.max(s2);
    let precision = (p1 - s1).max(p2 - s2) + scale + 1;
    adjust_precision_scale(precision, scale)
}

/// `findRoundDecimalType`: `ROUND(DECIMAL(p, s), round)`.
pub fn round_decimal(precision: u8, scale: u8, round: i32) -> Decimal {
    let (p, s) = (precision as i32, scale as i32);
    if round >= s {
        return Decimal::new(precision, scale);
    }
    if round < 0 {
        return Decimal::new((MAX_DECIMAL_PRECISION as i32).min(1 + p - s) as u8, 0);
    }
    Decimal::new((1 + p - s + round) as u8, round as u8)
}

/// `findAvgAggType`: `AVG(DECIMAL(p, s))` is `DECIMAL(38, max(6, s))`; other types keep theirs.
pub fn avg_agg_type(arg: &LogicalType) -> LogicalType {
    let result = match arg.kind {
        LogicalKind::Decimal { scale, .. } => division_decimal(38, scale, 20, 0).to_type(),
        _ => arg.clone(),
    };
    result.with_nullable(arg.nullable)
}

/// `findSumAggType`: `SUM(DECIMAL(p, s))` is `DECIMAL(38, s)`; other types keep theirs.
pub fn sum_agg_type(arg: &LogicalType) -> LogicalType {
    let result = match arg.kind {
        LogicalKind::Decimal { scale, .. } => Decimal::new(38, scale).to_type(),
        _ => arg.clone(),
    };
    result.with_nullable(arg.nullable)
}

/// The decimal view of an exact numeric type (`TINYINT` is `DECIMAL(3, 0)`, ..., `BIGINT` is
/// `DECIMAL(19, 0)`), as Calcite's `decimalOf` does for `FlinkTypeSystem.deriveDecimal*Type`.
pub fn as_decimal(ty: &LogicalType) -> Option<Decimal> {
    if !ty.is(F::ExactNumeric) {
        return None;
    }
    Some(Decimal::new(ty.precision()?, ty.scale()?))
}

/// `LogicalTypeMerging.findCommonType`: the most general type all of `types` can be cast to, or
/// `None` (also when every type is `NULL`).
pub fn find_common_type(types: &[LogicalType]) -> Option<LogicalType> {
    assert!(!types.is_empty(), "List of types must not be empty.");
    let has_raw = types.iter().any(|t| t.root() == R::Raw);
    let has_null = types.iter().any(|t| t.root() == R::Null);
    let has_nullable = types.iter().any(|t| t.nullable);
    let normalized: Vec<LogicalType> = types.iter().map(|t| t.with_nullable(true)).collect();
    let found = find_common_nullable_type(&normalized, has_raw, has_null)
        .or_else(|| find_common_castable_type(&normalized))?;
    let found = found.with_nullable(has_nullable);
    if found.root() == R::Null {
        return None;
    }
    Some(found)
}

fn find_common_castable_type(types: &[LogicalType]) -> Option<LogicalType> {
    let mut result = types[0].clone();
    for ty in types {
        if ty.root() == R::Null {
            continue;
        }
        if super::casts::supports_implicit_cast(&result, ty) {
            result = ty.clone();
        } else if !super::casts::supports_implicit_cast(ty, &result) {
            return None;
        }
    }
    Some(result)
}

fn find_common_nullable_type(
    types: &[LogicalType],
    has_raw: bool,
    has_null: bool,
) -> Option<LogicalType> {
    if has_raw {
        let first = &types[0];
        return types.iter().all(|t| t == first).then(|| first.clone());
    }
    let mut result: Option<LogicalType> = None;
    for ty in types {
        let root = ty.root();
        if root == R::Null {
            continue;
        }
        let current = result.get_or_insert_with(|| ty.clone()).clone();
        if let Some(pattern) = find_common_type_pattern(&current, ty) {
            result = Some(pattern);
            continue;
        }
        match root {
            R::Array => {
                return find_common_children(types).map(|c| LogicalType::array(c[0].clone()));
            }
            R::Multiset => {
                return find_common_children(types).map(|c| LogicalType::multiset(c[0].clone()));
            }
            R::Map => {
                return find_common_children(types)
                    .map(|c| LogicalType::map(c[0].clone(), c[1].clone()));
            }
            R::Row => return find_common_row_type(types),
            _ => {}
        }
        if !are_similar_types(&current, ty) {
            return None;
        }
        let next = if ty.is(F::CharacterString) || ty.is(F::BinaryString) {
            let length = current.length()?.max(ty.length()?);
            let current_root = current.root();
            if matches!(current_root, R::Varchar | R::Varbinary) {
                string_type(current_root, length)
            } else if current.length() != ty.length() {
                match current_root {
                    R::Char => string_type(R::Varchar, length),
                    R::Binary => string_type(R::Varbinary, length),
                    _ => current,
                }
            } else {
                string_type(root, length)
            }
        } else if ty.is(F::ExactNumeric) {
            if current.is(F::ExactNumeric) {
                common_exact_numeric(&current, ty)
            } else if current.is(F::ApproximateNumeric) {
                if root == R::Decimal {
                    LogicalType::double()
                } else {
                    current
                }
            } else {
                return None;
            }
        } else if ty.is(F::ApproximateNumeric) {
            if current.is(F::ApproximateNumeric) {
                if current.root() == R::Double || root == R::Double {
                    LogicalType::double()
                } else {
                    current
                }
            } else if current.is(F::ExactNumeric) {
                if current.root() == R::Decimal {
                    LogicalType::double()
                } else {
                    ty.clone()
                }
            } else {
                return None;
            }
        } else if root == R::Date {
            if current.root() == R::Date {
                LogicalType::date()
            } else {
                return None;
            }
        } else if ty.is(F::Time) {
            if current.is(F::Time) {
                LogicalType::time(current.precision()?.max(ty.precision()?))
            } else {
                return None;
            }
        } else if ty.is(F::Timestamp) {
            if current.is(F::Timestamp) {
                common_timestamp(&current, ty)
            } else {
                return None;
            }
        } else if root == R::IntervalDayTime {
            common_day_time(&current, ty)?
        } else if root == R::IntervalYearMonth {
            common_year_month(&current, ty)?
        } else {
            return None;
        };
        result = Some(next);
    }
    match result {
        None if has_null => Some(LogicalType::null()),
        other => other,
    }
}

/// `areSimilarTypes`: the two could be the operands of an equality predicate.
fn are_similar_types(left: &LogicalType, right: &LogicalType) -> bool {
    for family in [
        F::CharacterString,
        F::BinaryString,
        F::Numeric,
        F::Time,
        F::Timestamp,
    ] {
        if left.is(family) && right.is(family) {
            return true;
        }
    }
    left.root() == right.root()
}

/// `findCommonTypePattern`: `DATETIME` with `INTERVAL`, and `TIMESTAMP`/`DATE` with an exact
/// numeric (an exact numeric added to a date-time counts days).
fn find_common_type_pattern(result: &LogicalType, ty: &LogicalType) -> Option<LogicalType> {
    if result.is(F::Datetime) && ty.is(F::Interval) {
        Some(result.clone())
    } else if result.is(F::Interval) && ty.is(F::Datetime) {
        Some(ty.clone())
    } else if (result.is(F::Timestamp) || result.root() == R::Date) && ty.is(F::ExactNumeric) {
        Some(result.clone())
    } else if result.is(F::ExactNumeric) && (ty.is(F::Timestamp) || ty.root() == R::Date) {
        Some(ty.clone())
    } else {
        None
    }
}

fn find_common_children(types: &[LogicalType]) -> Option<Vec<LogicalType>> {
    let first = &types[0];
    let root = first.root();
    let n = first.children().len();
    if types
        .iter()
        .any(|t| t.root() != root || t.children().len() != n)
    {
        return None;
    }
    (0..n)
        .map(|i| {
            let column: Vec<LogicalType> = types.iter().map(|t| t.children()[i].clone()).collect();
            find_common_type(&column)
        })
        .collect()
}

fn find_common_row_type(types: &[LogicalType]) -> Option<LogicalType> {
    let children = find_common_children(types)?;
    let LogicalKind::Row(fields) = &types[0].kind else {
        return None;
    };
    Some(LogicalType::row(
        fields
            .iter()
            .zip(children)
            .map(|(f, ty)| RowField {
                name: f.name.clone(),
                ty,
                description: f.description.clone(),
            })
            .collect(),
    ))
}

fn common_exact_numeric(result: &LogicalType, ty: &LogicalType) -> LogicalType {
    if ty == result {
        return result.clone();
    }
    if result.root() != R::Decimal && ty.root() != R::Decimal {
        return if ty.precision() > result.precision() {
            ty.clone()
        } else {
            result.clone()
        };
    }
    let (p1, p2) = (
        result.precision().unwrap() as i32,
        ty.precision().unwrap() as i32,
    );
    let (s1, s2) = (result.scale().unwrap() as i32, ty.scale().unwrap() as i32);
    let max = MAX_DECIMAL_PRECISION as i32;
    let d = (p1 - s1).max(p2 - s2).min(max);
    let s = s1.max(s2).min(max - d);
    LogicalType::decimal((d + s) as u8, s as u8)
}

fn common_timestamp(result: &LogicalType, ty: &LogicalType) -> LogicalType {
    if ty == result {
        return result.clone();
    }
    let precision = result.precision().unwrap().max(ty.precision().unwrap());
    let (r, t) = (result.root(), ty.root());
    let root = if t == r {
        r
    } else if t == R::TimestampWithTimeZone || r == R::TimestampWithTimeZone {
        R::TimestampWithTimeZone
    } else if t == R::TimestampWithLocalTimeZone || r == R::TimestampWithLocalTimeZone {
        R::TimestampWithLocalTimeZone
    } else {
        R::TimestampWithoutTimeZone
    };
    match root {
        R::TimestampWithTimeZone => LogicalKind::TimestampTz(precision).into(),
        R::TimestampWithLocalTimeZone => LogicalKind::TimestampLtz {
            precision,
            kind: TimestampKind::Regular,
        }
        .into(),
        _ => LogicalType::timestamp(precision),
    }
}

fn string_type(root: R, length: u32) -> LogicalType {
    match root {
        R::Char => LogicalType::char(length),
        R::Varchar => LogicalType::varchar(length),
        R::Binary => LogicalKind::Binary(length).into(),
        _ => LogicalKind::Varbinary(length).into(),
    }
}

fn day_time_bounds(r: DayTimeResolution) -> (u8, u8) {
    // DAY=0, HOUR=1, MINUTE=2, SECOND=3
    use DayTimeResolution as D;
    match r {
        D::Day => (0, 0),
        D::DayToHour => (0, 1),
        D::DayToMinute => (0, 2),
        D::DayToSecond => (0, 3),
        D::Hour => (1, 1),
        D::HourToMinute => (1, 2),
        D::HourToSecond => (1, 3),
        D::Minute => (2, 2),
        D::MinuteToSecond => (2, 3),
        D::Second => (3, 3),
    }
}

fn day_time_from_bounds(start: u8, end: u8) -> Option<DayTimeResolution> {
    use DayTimeResolution as D;
    Some(match (start, end) {
        (0, 0) => D::Day,
        (0, 1) => D::DayToHour,
        (0, 2) => D::DayToMinute,
        (0, 3) => D::DayToSecond,
        (1, 1) => D::Hour,
        (1, 2) => D::HourToMinute,
        (1, 3) => D::HourToSecond,
        (2, 2) => D::Minute,
        (2, 3) => D::MinuteToSecond,
        (3, 3) => D::Second,
        _ => return None,
    })
}

fn common_day_time(result: &LogicalType, ty: &LogicalType) -> Option<LogicalType> {
    let (
        LogicalKind::IntervalDayTime {
            resolution: r1,
            day_precision: d1,
            fractional_precision: f1,
        },
        LogicalKind::IntervalDayTime {
            resolution: r2,
            day_precision: d2,
            fractional_precision: f2,
        },
    ) = (&result.kind, &ty.kind)
    else {
        return None;
    };
    let (a, b) = (day_time_bounds(*r1), day_time_bounds(*r2));
    let resolution = day_time_from_bounds(a.0.min(b.0), a.1.max(b.1))?;
    Some(
        LogicalKind::IntervalDayTime {
            resolution,
            day_precision: *d1.max(d2),
            fractional_precision: *f1.max(f2),
        }
        .into(),
    )
}

fn common_year_month(result: &LogicalType, ty: &LogicalType) -> Option<LogicalType> {
    let (
        LogicalKind::IntervalYearMonth {
            resolution: r1,
            year_precision: p1,
        },
        LogicalKind::IntervalYearMonth {
            resolution: r2,
            year_precision: p2,
        },
    ) = (&result.kind, &ty.kind)
    else {
        return None;
    };
    use YearMonthResolution as Y;
    let bounds = |r: &Y| match r {
        Y::Year => (0, 0),
        Y::YearToMonth => (0, 1),
        Y::Month => (1, 1),
    };
    let (a, b) = (bounds(r1), bounds(r2));
    let resolution = match (a.0.min(b.0), a.1.max(b.1)) {
        (0, 0) => Y::Year,
        (0, 1) => Y::YearToMonth,
        _ => Y::Month,
    };
    Some(
        LogicalKind::IntervalYearMonth {
            resolution,
            year_precision: *p1.max(p2),
        }
        .into(),
    )
}

#[cfg(test)]
mod tests {
    //! `LogicalTypeMergingTest` and `LogicalCommonTypeTest` (release-2.2.1), transcribed.
    use super::*;

    fn ty(sql: &str) -> LogicalType {
        LogicalType::from_syntax(&crate::parse_type_str(sql).unwrap()).unwrap()
    }

    fn pair(d: Decimal) -> (u8, u8) {
        (d.precision, d.scale)
    }

    #[test]
    fn logical_type_merging_test() {
        assert_eq!(pair(division_decimal(32, 8, 38, 8)), (38, 6));
        assert_eq!(pair(division_decimal(30, 20, 30, 20)), (38, 8));
        assert_eq!(pair(multiplication_decimal(30, 10, 30, 10)), (38, 6));
        assert_eq!(pair(multiplication_decimal(30, 20, 30, 20)), (38, 17));
        assert_eq!(pair(multiplication_decimal(38, 2, 38, 3)), (38, 5));
        assert_eq!(pair(modulo_decimal(30, 10, 30, 10)), (30, 10));
        assert_eq!(pair(modulo_decimal(30, 20, 25, 20)), (25, 20));
        assert_eq!(pair(modulo_decimal(10, 10, 10, 10)), (10, 10));
        assert_eq!(pair(addition_decimal(38, 8, 32, 8)), (38, 7));
        assert_eq!(pair(addition_decimal(32, 8, 38, 8)), (38, 7));
        assert_eq!(pair(addition_decimal(30, 20, 28, 20)), (31, 20));
        assert_eq!(pair(addition_decimal(10, 10, 10, 10)), (11, 10));
        assert_eq!(pair(addition_decimal(38, 5, 38, 4)), (38, 5));
        assert_eq!(pair(round_decimal(32, 8, 5)), (30, 5));
        assert_eq!(pair(round_decimal(32, 8, 10)), (32, 8));
        assert_eq!(pair(round_decimal(30, 20, 18)), (29, 18));
        assert_eq!(pair(round_decimal(10, 10, 2)), (3, 2));
        for ((p, s), expected) in [
            ((38, 20), (38, 20)),
            ((38, 2), (38, 6)),
            ((38, 8), (38, 8)),
            ((30, 20), (38, 20)),
            ((10, 10), (38, 10)),
        ] {
            let avg = avg_agg_type(&LogicalType::decimal(p, s).not_null());
            assert_eq!(avg, LogicalType::decimal(expected.0, expected.1).not_null());
        }
    }

    #[test]
    fn logical_common_type_test() {
        let null = LogicalType::null();
        let cases: Vec<(Vec<LogicalType>, Option<LogicalType>)> = vec![
            (vec![ty("INT"), ty("INT")], Some(ty("INT"))),
            (vec![ty("INT"), ty("ARRAY<INT>")], None),
            (vec![ty("INT"), ty("VARCHAR(23)")], None),
            (vec![ty("BINARY"), ty("VARCHAR(23)")], None),
            (
                vec![ty("INT NOT NULL"), ty("INT NOT NULL")],
                Some(ty("INT NOT NULL")),
            ),
            (vec![ty("INT"), ty("BIGINT NOT NULL")], Some(ty("BIGINT"))),
            (vec![null.clone(), null.clone()], None),
            (vec![null.clone(), ty("INT"), ty("INT")], Some(ty("INT"))),
            (
                vec![ty("ARRAY<INT>"), ty("ARRAY<INT>")],
                Some(ty("ARRAY<INT>")),
            ),
            (
                vec![ty("ARRAY<BIGINT>"), ty("ARRAY<INT>")],
                Some(ty("ARRAY<BIGINT>")),
            ),
            (
                vec![ty("MULTISET<BIGINT>"), ty("MULTISET<INT>")],
                Some(ty("MULTISET<BIGINT>")),
            ),
            (
                vec![ty("MAP<BIGINT, DOUBLE>"), ty("MAP<INT, DOUBLE>")],
                Some(ty("MAP<BIGINT, DOUBLE>")),
            ),
            (
                vec![
                    ty("ROW<f0 INT, f1 INT, f2 BIGINT>"),
                    ty("ROW<f0 BIGINT, f1 INT, f2 INT>"),
                ],
                Some(ty("ROW<f0 BIGINT, f1 INT, f2 BIGINT>")),
            ),
            (vec![ty("CHAR(2)"), ty("CHAR(2)")], Some(ty("CHAR(2)"))),
            (
                vec![LogicalType::char(0), LogicalType::char(0)],
                Some(LogicalType::char(0)),
            ),
            (vec![ty("CHAR(2)"), ty("CHAR(4)")], Some(ty("VARCHAR(4)"))),
            (vec![ty("VARCHAR(2)"), ty("STRING")], Some(ty("STRING"))),
            (
                vec![ty("VARCHAR(2)"), ty("CHAR(5)")],
                Some(ty("VARCHAR(5)")),
            ),
            (
                vec![ty("CHAR(5)"), ty("VARCHAR(2)"), ty("VARCHAR(7)")],
                Some(ty("VARCHAR(7)")),
            ),
            (
                vec![ty("BINARY(2)"), ty("BINARY(4)")],
                Some(ty("VARBINARY(4)")),
            ),
            (
                vec![ty("BINARY(5)"), ty("VARBINARY(2)"), ty("VARBINARY(7)")],
                Some(ty("VARBINARY(7)")),
            ),
            (vec![ty("DOUBLE"), ty("FLOAT")], Some(ty("DOUBLE"))),
            (vec![ty("DOUBLE"), ty("DECIMAL(2, 2)")], Some(ty("DOUBLE"))),
            (vec![ty("INT"), ty("FLOAT")], Some(ty("FLOAT"))),
            (
                vec![ty("DECIMAL(2, 2)"), ty("DOUBLE"), ty("FLOAT")],
                Some(ty("DOUBLE")),
            ),
            (
                vec![
                    ty("DECIMAL(2, 2)"),
                    ty("DECIMAL(5, 2)"),
                    ty("DECIMAL(7, 5)"),
                ],
                Some(ty("DECIMAL(8, 5)")),
            ),
            (
                vec![ty("DECIMAL(2, 2)"), ty("INT"), ty("BIGINT")],
                Some(ty("DECIMAL(21, 2)")),
            ),
            (vec![ty("DATE"), ty("DATE"), ty("TIME")], None),
            (
                vec![ty("TIME(3)"), ty("TIME(5)"), ty("TIME(2)")],
                Some(ty("TIME(5)")),
            ),
            (
                vec![ty("TIMESTAMP(3)"), ty("TIMESTAMP(5)"), ty("TIMESTAMP(2)")],
                Some(ty("TIMESTAMP(5)")),
            ),
            (
                vec![
                    ty("TIMESTAMP(3)"),
                    ty("TIMESTAMP(5) WITH TIME ZONE"),
                    ty("TIMESTAMP_LTZ(2)"),
                ],
                Some(ty("TIMESTAMP(5) WITH TIME ZONE")),
            ),
            (
                vec![ty("TIMESTAMP(3)"), ty("TIMESTAMP_LTZ(2)")],
                Some(ty("TIMESTAMP_LTZ(3)")),
            ),
            (vec![ty("INTERVAL DAY"), ty("DATE")], Some(ty("DATE"))),
            (vec![ty("INTERVAL MONTH"), ty("DATE")], Some(ty("DATE"))),
            (vec![ty("TIME"), ty("INTERVAL MINUTE")], Some(ty("TIME"))),
            (vec![ty("DATE"), ty("INTERVAL DAY")], Some(ty("DATE"))),
            (vec![ty("INT"), ty("DATE")], Some(ty("DATE"))),
            (vec![ty("TIME"), ty("DECIMAL")], None),
            (vec![ty("TIMESTAMP"), ty("DECIMAL")], Some(ty("TIMESTAMP"))),
            (
                vec![ty("INTERVAL DAY TO MINUTE"), ty("INTERVAL SECOND")],
                Some(ty("INTERVAL DAY TO SECOND")),
            ),
            (
                vec![ty("INTERVAL HOUR"), ty("INTERVAL SECOND(2, 0)")],
                Some(ty("INTERVAL HOUR TO SECOND(6)")),
            ),
            (
                vec![ty("INTERVAL MONTH"), ty("INTERVAL YEAR")],
                Some(ty("INTERVAL YEAR TO MONTH")),
            ),
        ];
        for (types, expected) in cases {
            assert_eq!(find_common_type(&types), expected, "{types:?}");
        }
    }
}
