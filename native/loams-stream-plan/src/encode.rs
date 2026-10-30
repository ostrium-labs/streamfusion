//! Scalar expressions in the engine's prefix encoding: the Rust port of StreamFusion's
//! `RexExpression` encoder (FC1 Task 10).
//!
//! The encoding is the engine's (`expr.rs`, `build_expr`): parallel `kinds`/`payload`/
//! `child_counts` arrays in pre-order, with literals in typed pools. Node kinds and call op codes
//! are the Java encoder's (its `KIND_*` constants and `opCode`).
//!
//! The Java encoder admits only what the native side evaluates with verified Flink parity and
//! hands everything else back to Flink's JVM code. Loams Stream has no JVM, so an expression the
//! port does not admit fails with a named feature (D2268); it is never evaluated approximately.

use std::sync::Arc;

use arrow_schema::{Field as ArrowField, Schema};
use loams_stream_sql::types::arrow::to_arrow_field;
use loams_stream_sql::types::{LogicalKind, LogicalType};

use crate::error::{PlanError, PlanResult};
use crate::expr::{Literal, Op, ScalarExpr, materialized};
use crate::opspec::{CalcSpec, EncodedExpr};

// Node kinds, mirrored on the native side (`RexExpression.KIND_*`).
pub const KIND_INPUT_REF: i64 = 0;
pub const KIND_LIT_LONG: i64 = 1;
pub const KIND_LIT_DOUBLE: i64 = 2;
pub const KIND_LIT_STRING: i64 = 3;
pub const KIND_LIT_BOOL: i64 = 4;
pub const KIND_LIT_NULL: i64 = 5;
pub const KIND_CALL: i64 = 6;
pub const KIND_LIT_INT: i64 = 7;
pub const KIND_LIT_SMALL: i64 = 8;
pub const KIND_LIT_TINY: i64 = 9;
pub const KIND_CAST: i64 = 11;
pub const KIND_PROCTIME: i64 = 12;
pub const KIND_FIELD_ACCESS: i64 = 13;
pub const KIND_CAST_DECIMAL: i64 = 14;
pub const KIND_LIT_DECIMAL: i64 = 16;
pub const KIND_CAST_NARROW: i64 = 18;
pub const KIND_ITEM: i64 = 19;
pub const KIND_DECIMAL_DIVIDE: i64 = 20;
pub const KIND_DECIMAL_MOD: i64 = 21;
pub const KIND_LIT_FLOAT: i64 = 22;
pub const KIND_LIT_BINARY: i64 = 23;
pub const KIND_LIT_TEMPORAL: i64 = 24;
pub const KIND_DECIMAL_ADD: i64 = 26;
pub const KIND_DECIMAL_SUBTRACT: i64 = 27;
pub const KIND_DECIMAL_MULTIPLY: i64 = 28;
pub const KIND_LIT_TIMESTAMP: i64 = 29;
pub const KIND_LIT_TYPED_NULL: i64 = 31;
pub const KIND_STRING_TO_INTEGER: i64 = 33;
pub const KIND_INTEGER_TO_STRING: i64 = 34;
pub const KIND_DECIMAL_FLOAT: i64 = 37;

// Call op codes (`RexExpression.opCode` and the function dispatch).
pub const OP_PLUS: i64 = 0;
pub const OP_MINUS: i64 = 1;
pub const OP_TIMES: i64 = 2;
pub const OP_DIVIDE: i64 = 3;
pub const OP_MOD: i64 = 4;
pub const OP_NEGATE: i64 = 5;
pub const OP_GT: i64 = 10;
pub const OP_GE: i64 = 11;
pub const OP_LT: i64 = 12;
pub const OP_LE: i64 = 13;
pub const OP_EQ: i64 = 14;
pub const OP_NE: i64 = 15;
pub const OP_AND: i64 = 20;
pub const OP_OR: i64 = 21;
pub const OP_NOT: i64 = 22;
pub const OP_IS_NULL: i64 = 30;
pub const OP_IS_NOT_NULL: i64 = 31;
pub const OP_IS_TRUE: i64 = 32;
pub const OP_IS_NOT_TRUE: i64 = 33;
pub const OP_IS_FALSE: i64 = 34;
pub const OP_IS_NOT_FALSE: i64 = 35;
pub const OP_CASE: i64 = 40;
pub const OP_CHAR_LENGTH: i64 = 52;
pub const OP_LIKE: i64 = 56;
pub const OP_POSITION: i64 = 57;
pub const OP_REPLACE: i64 = 58;
pub const OP_REVERSE: i64 = 59;
pub const OP_LTRIM: i64 = 60;
pub const OP_RTRIM: i64 = 61;
pub const OP_FLOAT_ABS: i64 = 62;
pub const OP_FLOAT_FLOOR: i64 = 63;
pub const OP_FLOAT_CEIL: i64 = 64;
pub const OP_SIGN: i64 = 65;
pub const OP_REPEAT: i64 = 66;
pub const OP_ASCII: i64 = 67;
pub const OP_CHR: i64 = 81;
pub const OP_DATE_FORMAT: i64 = 86;
pub const OP_TIMESTAMP_FROM_MILLIS: i64 = 87;
pub const OP_CONCAT: i64 = 93;
pub const OP_CONCAT_WS: i64 = 94;
pub const OP_TRIM_BOTH: i64 = 113;
pub const OP_SUBSTRING: i64 = 125;
pub const OP_LEFT: i64 = 126;
pub const OP_RIGHT: i64 = 127;
pub const OP_LPAD: i64 = 128;
pub const OP_RPAD: i64 = 129;
pub const OP_SPLIT_INDEX: i64 = 130;
pub const OP_TRIM_LEADING: i64 = 139;
pub const OP_TRIM_TRAILING: i64 = 140;
pub const OP_IF_NULL: i64 = 158;
pub const OP_EXACT_ABS: i64 = 218;
pub const OP_EXACT_NEGATE: i64 = 219;
/// `TO_TIMESTAMP(string)` with Flink's default format (a Loams Stream engine kernel).
pub const OP_TO_TIMESTAMP: i64 = 900;

// Cast target codes (`RexExpression.CAST_*`).
const CAST_TINYINT: i64 = 0;
const CAST_SMALLINT: i64 = 1;
const CAST_INTEGER: i64 = 2;
const CAST_BIGINT: i64 = 3;
const CAST_FLOAT: i64 = 4;
const CAST_DOUBLE: i64 = 5;

/// Temporal literal type codes (`RexExpression.temporalTypeCode`).
const TEMPORAL_DATE: i64 = 9;
const TEMPORAL_TIME: i64 = 10;
const TEMPORAL_MONTHS: i64 = 12;
const TEMPORAL_MILLIS: i64 = 13;

/// A widening order over the numeric types; `None` if not numeric (`numericRank`).
fn numeric_rank(ty: &LogicalType) -> Option<i64> {
    Some(match ty.kind {
        LogicalKind::TinyInt => 0,
        LogicalKind::SmallInt => 1,
        LogicalKind::Int => 2,
        LogicalKind::BigInt => 3,
        LogicalKind::Float => 4,
        LogicalKind::Double => 5,
        _ => return None,
    })
}

fn is_character(ty: &LogicalType) -> bool {
    matches!(ty.kind, LogicalKind::Char(_) | LogicalKind::Varchar(_))
}

fn is_integer(ty: &LogicalType) -> bool {
    matches!(numeric_rank(ty), Some(0..=3))
}

fn is_int32(ty: &LogicalType) -> bool {
    matches!(
        ty.kind,
        LogicalKind::TinyInt | LogicalKind::SmallInt | LogicalKind::Int | LogicalKind::Null
    )
}

fn is_float(ty: &LogicalType) -> bool {
    matches!(ty.kind, LogicalKind::Float | LogicalKind::Double)
}

fn is_decimal(ty: &LogicalType) -> bool {
    matches!(ty.kind, LogicalKind::Decimal { .. })
}

fn decimal_payload(ty: &LogicalType) -> i64 {
    match ty.kind {
        LogicalKind::Decimal { precision, scale } => precision as i64 * 100 + scale as i64,
        _ => 0,
    }
}

/// The encoder's state: the arrays being written.
#[derive(Debug, Default)]
pub struct Encoder {
    out: EncodedExpr,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(self) -> EncodedExpr {
        self.out
    }

    fn add(&mut self, kind: i64, payload: i64, children: usize) {
        self.out.kinds.push(kind);
        self.out.payload.push(payload);
        self.out.child_counts.push(children as i64);
    }

    fn long(&mut self, v: i64) -> i64 {
        self.out.longs.push(v);
        self.out.longs.len() as i64 - 1
    }

    fn double(&mut self, v: f64) -> i64 {
        self.out.doubles.push(v);
        self.out.doubles.len() as i64 - 1
    }

    fn string(&mut self, v: Option<String>) -> i64 {
        self.out.strings.push(v);
        self.out.strings.len() as i64 - 1
    }

    /// Encodes `e` and returns its root node.
    pub fn root(&mut self, e: &ScalarExpr) -> PlanResult<usize> {
        let root = self.out.kinds.len();
        self.emit(e)?;
        Ok(root)
    }

    fn emit(&mut self, e: &ScalarExpr) -> PlanResult<()> {
        match e {
            ScalarExpr::Input { index, .. } => {
                self.add(KIND_INPUT_REF, *index as i64, 0);
                Ok(())
            }
            ScalarExpr::Literal { value, ty } => self.literal(value, ty),
            ScalarExpr::Call { op, args, ty } => self.call(op, args, ty),
        }
    }

    fn literal(&mut self, value: &Literal, ty: &LogicalType) -> PlanResult<()> {
        match (value, &ty.kind) {
            (Literal::Null, LogicalKind::Null) => self.add(KIND_LIT_NULL, -1, 0),
            (Literal::Null, LogicalKind::Char(_) | LogicalKind::Varchar(_)) => {
                let i = self.string(None);
                self.add(KIND_LIT_STRING, i, 0);
            }
            (Literal::Null, LogicalKind::Varbinary(_)) => {
                let i = self.long(-1);
                self.add(KIND_LIT_BINARY, i, 0);
            }
            (Literal::Null, LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. }) => {
                let i = self.long(0);
                self.long(0);
                self.long(0);
                self.add(KIND_LIT_TIMESTAMP, i, 0);
            }
            (Literal::Null, LogicalKind::Date) => self.temporal(TEMPORAL_DATE, None),
            (Literal::Null, LogicalKind::Time(_)) => self.temporal(TEMPORAL_TIME, None),
            (Literal::Null, LogicalKind::IntervalYearMonth { .. }) => {
                self.temporal(TEMPORAL_MONTHS, None)
            }
            (Literal::Null, LogicalKind::IntervalDayTime { .. }) => {
                self.temporal(TEMPORAL_MILLIS, None)
            }
            (Literal::Null, _) => {
                let schema = typed_null_schema(ty)?;
                let i = self.string(Some(schema));
                self.add(KIND_LIT_TYPED_NULL, i, 0);
            }
            (Literal::Boolean(b), _) => {
                let i = self.long(*b as i64);
                self.add(KIND_LIT_BOOL, i, 0);
            }
            (Literal::Integer(v), LogicalKind::TinyInt) => self.int_literal(KIND_LIT_TINY, *v),
            (Literal::Integer(v), LogicalKind::SmallInt) => self.int_literal(KIND_LIT_SMALL, *v),
            (Literal::Integer(v), LogicalKind::Int) => self.int_literal(KIND_LIT_INT, *v),
            (Literal::Integer(v), LogicalKind::BigInt) => self.int_literal(KIND_LIT_LONG, *v),
            (Literal::Integer(v), LogicalKind::Decimal { precision, scale }) => {
                self.decimal_literal(*v as i128, 0, *precision, *scale)
            }
            (
                Literal::Decimal { unscaled, scale },
                LogicalKind::Decimal {
                    precision,
                    scale: s,
                },
            ) => self.decimal_literal(*unscaled, *scale, *precision, *s),
            (Literal::Double(v), LogicalKind::Float) => {
                let i = self.double(*v);
                self.add(KIND_LIT_FLOAT, i, 0);
            }
            (Literal::Double(v), LogicalKind::Double) => {
                let i = self.double(*v);
                self.add(KIND_LIT_DOUBLE, i, 0);
            }
            (Literal::String(s), LogicalKind::Char(_) | LogicalKind::Varchar(_)) => {
                let i = self.string(Some(s.clone()));
                self.add(KIND_LIT_STRING, i, 0);
            }
            (Literal::Binary(bytes), LogicalKind::Binary(_) | LogicalKind::Varbinary(_)) => {
                let i = self.long(bytes.len() as i64);
                for b in bytes {
                    self.long(*b as i64);
                }
                self.add(KIND_LIT_BINARY, i, 0);
            }
            (Literal::Date(d), LogicalKind::Date) => self.temporal(TEMPORAL_DATE, Some(*d as i64)),
            (Literal::Time(t), LogicalKind::Time(_)) => {
                self.temporal(TEMPORAL_TIME, Some(*t as i64))
            }
            (Literal::Months(m), LogicalKind::IntervalYearMonth { .. }) => {
                self.temporal(TEMPORAL_MONTHS, Some(*m as i64))
            }
            (Literal::Millis(ms), LogicalKind::IntervalDayTime { .. }) => {
                self.temporal(TEMPORAL_MILLIS, Some(*ms))
            }
            (
                Literal::Timestamp {
                    millis,
                    nano_of_milli,
                },
                LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. },
            ) => {
                let i = self.long(1);
                self.long(*millis);
                self.long(*nano_of_milli as i64);
                self.add(KIND_LIT_TIMESTAMP, i, 0);
            }
            (value, _) => {
                return Err(PlanError::not_yet(format!(
                    "native expression: literal {value:?} of type {ty}"
                )));
            }
        }
        Ok(())
    }

    fn int_literal(&mut self, kind: i64, v: i64) {
        let i = self.long(v);
        self.add(kind, i, 0);
    }

    fn temporal(&mut self, code: i64, value: Option<i64>) {
        let i = self.long(code);
        self.long(value.is_some() as i64);
        self.long(value.unwrap_or(0));
        self.add(KIND_LIT_TEMPORAL, i, 0);
    }

    /// A decimal literal at the type's scale, rounded HALF_UP; NULL when it overflows the
    /// precision (`emitLiteral`'s DECIMAL case).
    fn decimal_literal(&mut self, unscaled: i128, scale: u8, precision: u8, target: u8) {
        let mut v = unscaled;
        if target >= scale {
            for _ in scale..target {
                v = v.saturating_mul(10);
            }
        } else {
            let mut div = 1i128;
            for _ in target..scale {
                div *= 10;
            }
            let q = v / div;
            let r = (v % div).abs();
            v = if r * 2 >= div { q + v.signum() } else { q };
        }
        let digits = if v == 0 {
            1
        } else {
            v.unsigned_abs().to_string().len()
        };
        if digits > precision as usize {
            self.add(KIND_CAST_DECIMAL, precision as i64 * 100 + target as i64, 1);
            self.add(KIND_LIT_NULL, -1, 0);
            return;
        }
        let i = self.string(Some(format!("{v}|{precision}|{target}")));
        self.add(KIND_LIT_DECIMAL, i, 0);
    }

    fn call_node(&mut self, op: i64, args: &[ScalarExpr]) -> PlanResult<()> {
        self.add(KIND_CALL, op, args.len());
        for a in args {
            self.emit(a)?;
        }
        Ok(())
    }

    fn call(&mut self, op: &Op, args: &[ScalarExpr], ty: &LogicalType) -> PlanResult<()> {
        let not_yet = |what: String| Err(PlanError::not_yet(format!("native expression: {what}")));
        match op {
            Op::Plus | Op::Minus | Op::Times | Op::Divide | Op::Mod => {
                let [l, r] = args else {
                    return not_yet(format!("{op:?} of {} operands", args.len()));
                };
                let temporal = |t: &LogicalType| {
                    matches!(
                        t.kind,
                        LogicalKind::Date
                            | LogicalKind::Time(_)
                            | LogicalKind::Timestamp { .. }
                            | LogicalKind::TimestampLtz { .. }
                            | LogicalKind::IntervalDayTime { .. }
                            | LogicalKind::IntervalYearMonth { .. }
                    )
                };
                if temporal(l.ty()) || temporal(r.ty()) {
                    return self.temporal_arithmetic(op, l, r);
                }
                if is_decimal(ty) {
                    let kind = match op {
                        Op::Plus => KIND_DECIMAL_ADD,
                        Op::Minus => KIND_DECIMAL_SUBTRACT,
                        Op::Times => KIND_DECIMAL_MULTIPLY,
                        Op::Divide => KIND_DECIMAL_DIVIDE,
                        _ => KIND_DECIMAL_MOD,
                    };
                    for x in [l, r] {
                        if !is_decimal(x.ty()) && !is_integer(x.ty()) {
                            return not_yet(format!("decimal arithmetic over {}", x.ty()));
                        }
                    }
                    self.add(kind, decimal_payload(ty), 2);
                    self.emit(l)?;
                    return self.emit(r);
                }
                if numeric_rank(ty).is_none() {
                    return not_yet(format!("{op:?} of type {ty}"));
                }
                let narrow = match ty.kind {
                    LogicalKind::TinyInt => Some(CAST_TINYINT),
                    LogicalKind::SmallInt => Some(CAST_SMALLINT),
                    LogicalKind::Int => Some(CAST_INTEGER),
                    _ => None,
                };
                if let Some(n) = narrow {
                    self.add(KIND_CAST_NARROW, n, 1);
                }
                let code = match op {
                    Op::Plus => OP_PLUS,
                    Op::Minus => OP_MINUS,
                    Op::Times => OP_TIMES,
                    Op::Divide => OP_DIVIDE,
                    _ => OP_MOD,
                };
                self.call_node(code, args)
            }
            Op::Negate => {
                let [x] = args else {
                    return not_yet("negation arity".into());
                };
                if is_decimal(x.ty()) {
                    return self.call_node(OP_EXACT_NEGATE, args);
                }
                if is_integer(x.ty()) {
                    let code = if x.as_input().is_some() {
                        OP_NEGATE
                    } else {
                        OP_EXACT_NEGATE
                    };
                    return self.call_node(code, args);
                }
                if is_float(x.ty()) {
                    return self.call_node(OP_NEGATE, args);
                }
                not_yet(format!("negation of {}", x.ty()))
            }
            Op::Eq | Op::NotEq | Op::Gt | Op::Ge | Op::Lt | Op::Le => {
                let [l, r] = args else {
                    return not_yet("comparison arity".into());
                };
                let lt = materialized(l.ty());
                let rt = materialized(r.ty());
                let numeric = |t: &LogicalType| numeric_rank(t).is_some() || is_decimal(t);
                let comparable = (numeric(&lt) && numeric(&rt))
                    || (is_character(&lt) && is_character(&rt))
                    || lt.kind == rt.kind
                    || matches!(
                        (&lt.kind, &rt.kind),
                        (LogicalKind::Boolean, LogicalKind::Boolean)
                            | (LogicalKind::Null, _)
                            | (_, LogicalKind::Null)
                    );
                if !comparable {
                    return not_yet(format!("comparison of {lt} and {rt}"));
                }
                let timestamp = |t: &LogicalType| {
                    matches!(
                        t.kind,
                        LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. }
                    )
                };
                // Timestamps compare as their (millis, nano_of_milli) components, in order; a
                // TIMESTAMP against a TIMESTAMP_LTZ needs a zone and is not admitted.
                if (timestamp(&lt) || timestamp(&rt))
                    && std::mem::discriminant(&lt.kind) != std::mem::discriminant(&rt.kind)
                {
                    return not_yet(format!("comparison of {lt} and {rt}"));
                }
                let code = match op {
                    Op::Eq => OP_EQ,
                    Op::NotEq => OP_NE,
                    Op::Gt => OP_GT,
                    Op::Ge => OP_GE,
                    Op::Lt => OP_LT,
                    _ => OP_LE,
                };
                self.call_node(code, args)
            }
            Op::IsDistinctFrom | Op::IsNotDistinctFrom => {
                let [l, r] = args else {
                    return not_yet("IS DISTINCT FROM arity".into());
                };
                // CASE WHEN l IS NULL THEN r IS [NOT] NULL WHEN r IS NULL THEN distinct
                //      ELSE l <> r (or l = r) END
                let distinct = matches!(op, Op::IsDistinctFrom);
                let bool_ty = LogicalType::boolean().not_null();
                let null = |x: &ScalarExpr, negate: bool| {
                    ScalarExpr::call(
                        if negate { Op::IsNotNull } else { Op::IsNull },
                        vec![x.clone()],
                        bool_ty.clone(),
                    )
                };
                let cmp = ScalarExpr::call(
                    if distinct { Op::NotEq } else { Op::Eq },
                    vec![l.clone(), r.clone()],
                    LogicalType::boolean(),
                );
                let case = ScalarExpr::call(
                    Op::Case,
                    vec![
                        null(l, false),
                        null(r, distinct),
                        null(r, false),
                        ScalarExpr::bool(distinct),
                        cmp,
                    ],
                    bool_ty,
                );
                self.emit(&case)
            }
            Op::And | Op::Or => {
                if args.len() < 2 {
                    return not_yet("AND/OR arity".into());
                }
                let code = if matches!(op, Op::And) { OP_AND } else { OP_OR };
                // The native operator is binary: a left-deep nesting in pre-order is (n-1)
                // headers followed by the operands.
                for _ in 0..args.len() - 1 {
                    self.add(KIND_CALL, code, 2);
                }
                for a in args {
                    self.emit(a)?;
                }
                Ok(())
            }
            Op::Not => self.call_node(OP_NOT, args),
            Op::IsNull => self.call_node(OP_IS_NULL, args),
            Op::IsNotNull => self.call_node(OP_IS_NOT_NULL, args),
            Op::IsTrue => self.call_node(OP_IS_TRUE, args),
            Op::IsNotTrue => self.call_node(OP_IS_NOT_TRUE, args),
            Op::IsFalse => self.call_node(OP_IS_FALSE, args),
            Op::IsNotFalse => self.call_node(OP_IS_NOT_FALSE, args),
            Op::Case => {
                if args.is_empty() {
                    return not_yet("empty CASE".into());
                }
                self.call_node(OP_CASE, args)
            }
            Op::Like => {
                if args.len() != 2 || !args.iter().all(|a| is_character(a.ty())) {
                    return not_yet("LIKE with ESCAPE".into());
                }
                self.call_node(OP_LIKE, args)
            }
            Op::SimilarTo => not_yet("SIMILAR TO".into()),
            Op::Cast { try_cast } => self.cast(&args[0], ty, *try_cast),
            Op::Reinterpret => self.emit(&args[0]),
            Op::FieldAccess(name) => {
                let i = self.string(Some(name.clone()));
                self.add(KIND_FIELD_ACCESS, i, 1);
                self.emit(&args[0])
            }
            Op::Item => {
                if !matches!(
                    args[0].ty().kind,
                    LogicalKind::Array(_) | LogicalKind::Map(..)
                ) {
                    return not_yet("ITEM".into());
                }
                self.add(KIND_ITEM, 0, 2);
                self.emit(&args[0])?;
                self.emit(&args[1])
            }
            Op::Proctime => {
                self.add(KIND_PROCTIME, 0, 0);
                Ok(())
            }
            Op::Function(name) => self.function(name, args, ty),
        }
    }

    /// `datetime ± interval` (`nativeIntervalArithmetic`): the engine's temporal kernels take the
    /// datetime, the interval and whether to subtract.
    fn temporal_arithmetic(&mut self, op: &Op, l: &ScalarExpr, r: &ScalarExpr) -> PlanResult<()> {
        let interval = |t: &LogicalType| {
            matches!(
                t.kind,
                LogicalKind::IntervalDayTime { .. } | LogicalKind::IntervalYearMonth { .. }
            )
        };
        let (value, delta) = match op {
            Op::Plus | Op::Minus if interval(r.ty()) && !interval(l.ty()) => (l, r),
            Op::Plus if interval(l.ty()) && !interval(r.ty()) => (r, l),
            _ => {
                return Err(PlanError::not_yet(format!(
                    "native expression: {op:?} of {} and {}",
                    l.ty(),
                    r.ty()
                )));
            }
        };
        let months = matches!(delta.ty().kind, LogicalKind::IntervalYearMonth { .. });
        let code = match (&value.ty().kind, months) {
            (LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. }, false) => 210,
            (LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. }, true) => 214,
            (LogicalKind::Date, true) => 213,
            (LogicalKind::Date, false) => 211,
            (LogicalKind::Time(_), false) => 212,
            _ => {
                return Err(PlanError::not_yet(format!(
                    "native expression: {op:?} of {} and {}",
                    l.ty(),
                    r.ty()
                )));
            }
        };
        self.add(KIND_CALL, code, 3);
        self.emit(value)?;
        self.emit(delta)?;
        let i = self.long(matches!(op, Op::Minus) as i64);
        self.add(KIND_LIT_BOOL, i, 0);
        Ok(())
    }

    fn cast(&mut self, x: &ScalarExpr, ty: &LogicalType, try_cast: bool) -> PlanResult<()> {
        let not_yet = |what: String| Err(PlanError::not_yet(format!("native expression: {what}")));
        let from = materialized(x.ty());
        let to = materialized(ty);
        if from.kind == to.kind {
            return self.emit(x);
        }
        if matches!(from.kind, LogicalKind::Null) {
            return self.literal(&Literal::Null, ty);
        }
        let source = numeric_rank(&from);
        let target = numeric_rank(&to);
        if is_character(&from) && matches!(target, Some(0..=3)) {
            // Flink 2.2's default (non-legacy) cast fails on a bad number; TRY_CAST gives NULL.
            let flags = target.unwrap() | if try_cast { 4 } else { 0 };
            self.add(KIND_STRING_TO_INTEGER, flags, 1);
            return self.emit(x);
        }
        if matches!(source, Some(0..=3)) && is_character(&to) {
            let (length, pad) = match to.kind {
                LogicalKind::Char(n) => (n as i64, true),
                LogicalKind::Varchar(n) => (n as i64, false),
                _ => unreachable!(),
            };
            self.add(
                KIND_INTEGER_TO_STRING,
                if pad { -length } else { length },
                1,
            );
            return self.emit(x);
        }
        if try_cast && !(is_decimal(&from) && is_decimal(&to)) {
            return not_yet(format!("TRY_CAST {from} to {to}"));
        }
        if is_character(&from)
            && matches!(to.kind, LogicalKind::Varchar(_))
            && char_length(&to) >= char_length(&from)
        {
            return self.emit(x);
        }
        if is_decimal(&from) && is_float(&to) {
            let code = if matches!(to.kind, LogicalKind::Double) {
                CAST_DOUBLE
            } else {
                CAST_FLOAT
            };
            self.add(KIND_DECIMAL_FLOAT, code, 1);
            return self.emit(x);
        }
        if is_decimal(&to) && (is_decimal(&from) || matches!(source, Some(0..=3))) {
            self.add(KIND_CAST_DECIMAL, decimal_payload(&to), 1);
            return self.emit(x);
        }
        if let (Some(s), Some(t)) = (source, target)
            && t >= s
        {
            let code = match to.kind {
                LogicalKind::TinyInt => CAST_TINYINT,
                LogicalKind::SmallInt => CAST_SMALLINT,
                LogicalKind::Int => CAST_INTEGER,
                LogicalKind::BigInt => CAST_BIGINT,
                LogicalKind::Float => CAST_FLOAT,
                _ => CAST_DOUBLE,
            };
            self.add(KIND_CAST, code, 1);
            return self.emit(x);
        }
        let from_rank = if is_decimal(&from) { Some(4) } else { source };
        if let (Some(s), Some(t)) = (from_rank, target)
            && t <= 3
            && s > t
        {
            self.add(KIND_CAST_NARROW, t, 1);
            return self.emit(x);
        }
        not_yet(format!("CAST {from} to {to}"))
    }

    fn function(&mut self, name: &str, args: &[ScalarExpr], ty: &LogicalType) -> PlanResult<()> {
        let not_yet = |what: String| Err(PlanError::not_yet(format!("native expression: {what}")));
        let all_character = |args: &[ScalarExpr]| {
            args.iter()
                .all(|a| is_character(a.ty()) || matches!(a.ty().kind, LogicalKind::Null))
        };
        match name {
            "COALESCE" => {
                if args.len() < 2 || !args.iter().all(ScalarExpr::is_deterministic) {
                    return not_yet("COALESCE".into());
                }
                let n = args.len();
                self.add(KIND_CALL, OP_CASE, 2 * (n - 1) + 1);
                for a in &args[..n - 1] {
                    self.add(KIND_CALL, OP_IS_NOT_NULL, 1);
                    self.emit(a)?;
                    self.emit(a)?;
                }
                self.emit(&args[n - 1])
            }
            "IF" => {
                if args.len() != 3
                    || !args.iter().all(ScalarExpr::is_deterministic)
                    || args[1..]
                        .iter()
                        .any(|a| materialized(a.ty()).kind != materialized(ty).kind)
                {
                    return not_yet("IF with differently typed branches".into());
                }
                self.call_node(OP_CASE, args)
            }
            "IFNULL" => self.call_node(OP_IF_NULL, args),
            "CONCAT" if all_character(args) => self.call_node(OP_CONCAT, args),
            "CONCAT_WS" if all_character(args) => self.call_node(OP_CONCAT_WS, args),
            "CHAR_LENGTH" | "CHARACTER_LENGTH" if args.len() == 1 => {
                self.call_node(OP_CHAR_LENGTH, args)
            }
            "REVERSE" if args.len() == 1 => self.call_node(OP_REVERSE, args),
            "LTRIM" if args.len() == 1 => self.call_node(OP_LTRIM, args),
            "RTRIM" if args.len() == 1 => self.call_node(OP_RTRIM, args),
            "ASCII" if args.len() == 1 => self.call_node(OP_ASCII, args),
            "CHR" if args.len() == 1 => self.call_node(OP_CHR, args),
            "REPLACE" if args.len() == 3 => self.call_node(OP_REPLACE, args),
            "REPEAT" if args.len() == 2 => self.call_node(OP_REPEAT, args),
            "POSITION" if args.len() == 2 => self.call_node(OP_POSITION, args),
            "SUBSTRING" | "SUBSTR"
                if (2..=3).contains(&args.len())
                    && is_character(args[0].ty())
                    && args[1..].iter().all(|a| is_int32(a.ty())) =>
            {
                self.call_node(OP_SUBSTRING, args)
            }
            "LEFT" | "RIGHT"
                if args.len() == 2 && is_character(args[0].ty()) && is_int32(args[1].ty()) =>
            {
                self.call_node(if name == "LEFT" { OP_LEFT } else { OP_RIGHT }, args)
            }
            "LPAD" | "RPAD"
                if args.len() == 3
                    && is_character(args[0].ty())
                    && is_int32(args[1].ty())
                    && is_character(args[2].ty()) =>
            {
                self.call_node(if name == "LPAD" { OP_LPAD } else { OP_RPAD }, args)
            }
            "SPLIT_INDEX"
                if args.len() == 3
                    && is_character(args[0].ty())
                    && is_character(args[1].ty())
                    && is_int32(args[2].ty()) =>
            {
                self.call_node(OP_SPLIT_INDEX, args)
            }
            "TRIM" => {
                let [side, what, value] = args else {
                    return not_yet("TRIM".into());
                };
                let ScalarExpr::Literal {
                    value: Literal::Symbol(side),
                    ..
                } = side
                else {
                    return not_yet("TRIM".into());
                };
                if !matches!(what, ScalarExpr::Literal { .. }) || !is_character(value.ty()) {
                    return not_yet("TRIM of a non-literal set".into());
                }
                let code = match side.as_str() {
                    "LEADING" => OP_TRIM_LEADING,
                    "TRAILING" => OP_TRIM_TRAILING,
                    _ => OP_TRIM_BOTH,
                };
                self.add(KIND_CALL, code, 2);
                self.emit(value)?;
                self.emit(what)
            }
            "ABS" if args.len() == 1 => {
                if is_integer(args[0].ty()) || is_decimal(args[0].ty()) {
                    self.call_node(OP_EXACT_ABS, args)
                } else if is_float(args[0].ty()) {
                    self.call_node(OP_FLOAT_ABS, args)
                } else {
                    not_yet(format!("ABS of {}", args[0].ty()))
                }
            }
            "FLOOR" | "CEIL" | "CEILING" if args.len() == 1 => {
                if is_integer(args[0].ty()) {
                    self.emit(&args[0])
                } else if is_float(args[0].ty()) {
                    let code = if name == "FLOOR" {
                        OP_FLOAT_FLOOR
                    } else {
                        OP_FLOAT_CEIL
                    };
                    self.call_node(code, args)
                } else {
                    not_yet(format!("{name} of {}", args[0].ty()))
                }
            }
            "SIGN" if args.len() == 1 => self.call_node(OP_SIGN, args),
            crate::lower::TIMESTAMP_FROM_MILLIS => self.call_node(OP_TIMESTAMP_FROM_MILLIS, args),
            "EXTRACT" | "YEAR" | "QUARTER" | "MONTH" | "WEEK" | "DAYOFYEAR" | "DAYOFMONTH"
            | "DAYOFWEEK" | "HOUR" | "MINUTE" | "SECOND" => {
                let (unit, source) = match (name, args) {
                    (
                        "EXTRACT",
                        [
                            ScalarExpr::Literal {
                                value: Literal::Symbol(unit),
                                ..
                            },
                            source,
                        ],
                    ) => (unit.as_str(), source),
                    ("EXTRACT", _) => return not_yet("EXTRACT form".into()),
                    (_, [source]) => (
                        match name {
                            "DAYOFYEAR" => "DOY",
                            "DAYOFMONTH" => "DAY",
                            "DAYOFWEEK" => "DOW",
                            other => other,
                        },
                        source,
                    ),
                    _ => return not_yet(format!("{name} arity")),
                };
                let op = match unit {
                    "YEAR" => 203,
                    "MONTH" => 204,
                    "DAY" => 205,
                    "HOUR" => 206,
                    "MINUTE" => 207,
                    "SECOND" => 208,
                    "QUARTER" => 133,
                    "WEEK" => 134,
                    "DOY" => 135,
                    "DOW" => 136,
                    other => return not_yet(format!("EXTRACT({other} ...)")),
                };
                // A TIMESTAMP_LTZ reads in the session zone, UTC in the harness.
                if !matches!(
                    source.ty().kind,
                    LogicalKind::Date
                        | LogicalKind::Time(_)
                        | LogicalKind::Timestamp { .. }
                        | LogicalKind::TimestampLtz { .. }
                ) {
                    return not_yet(format!("EXTRACT from {}", source.ty()));
                }
                self.call_node(op, std::slice::from_ref(source))
            }
            "DATE_FORMAT" => {
                let [
                    ts,
                    ScalarExpr::Literal {
                        value: Literal::String(pattern),
                        ..
                    },
                ] = args
                else {
                    return not_yet("DATE_FORMAT with a non-literal format".into());
                };
                if !matches!(
                    ts.ty().kind,
                    LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. }
                ) {
                    return not_yet(format!("DATE_FORMAT of {}", ts.ty()));
                }
                let Some(chrono) = chrono_format(pattern) else {
                    return not_yet(format!("DATE_FORMAT pattern '{pattern}'"));
                };
                self.add(KIND_CALL, OP_DATE_FORMAT, 2);
                self.emit(ts)?;
                let i = self.string(Some(chrono));
                self.add(KIND_LIT_STRING, i, 0);
                Ok(())
            }
            "TO_TIMESTAMP" | "TO_TIMESTAMP_LTZ"
                if is_character(args[0].ty())
                    && (args.len() == 1
                        || args.len() == 2
                            && matches!(&args[1], ScalarExpr::Literal {
                                value: Literal::String(f), ..
                            } if f == "yyyy-MM-dd HH:mm:ss")) =>
            {
                // The session zone is UTC (the harness's default), so an LTZ parses as a local
                // timestamp does.
                self.call_node(OP_TO_TIMESTAMP, &args[..1])
            }
            "TO_TIMESTAMP_LTZ" if args.len() == 2 && is_integer(args[0].ty()) => {
                let factor = match &args[1] {
                    ScalarExpr::Literal {
                        value: Literal::Integer(3),
                        ..
                    } => 1,
                    ScalarExpr::Literal {
                        value: Literal::Integer(0),
                        ..
                    } => 1000,
                    _ => {
                        return not_yet(
                            "TO_TIMESTAMP_LTZ with a precision other than 0 or 3".into(),
                        );
                    }
                };
                let bigint = LogicalType::bigint().with_nullable(args[0].ty().nullable);
                let mut millis = ScalarExpr::call(
                    Op::Cast { try_cast: false },
                    vec![args[0].clone()],
                    bigint.clone(),
                );
                if factor != 1 {
                    millis = ScalarExpr::call(
                        Op::Times,
                        vec![
                            millis,
                            ScalarExpr::literal(
                                Literal::Integer(factor),
                                LogicalType::bigint().not_null(),
                            ),
                        ],
                        bigint,
                    );
                }
                self.call_node(OP_TIMESTAMP_FROM_MILLIS, &[millis])
            }
            other => not_yet(format!("function {other}")),
        }
    }
}

/// A Java `SimpleDateFormat` pattern as a chrono pattern, for the fields the engine formats
/// exactly (`toChronoFormat`); `None` for any other letter.
fn chrono_format(pattern: &str) -> Option<String> {
    if pattern.is_empty() {
        return None;
    }
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_alphabetic() {
            let mut j = i;
            while j < chars.len() && chars[j] == c {
                j += 1;
            }
            let token: String = chars[i..j].iter().collect();
            out.push_str(match token.as_str() {
                "yyyy" => "%Y",
                "yy" => "%y",
                "MM" => "%m",
                "dd" => "%d",
                "HH" => "%H",
                "mm" => "%M",
                "ss" => "%S",
                _ => return None,
            });
            i = j;
        } else {
            if c == '%' {
                out.push_str("%%");
            } else {
                out.push(c);
            }
            i += 1;
        }
    }
    Some(out)
}

fn char_length(ty: &LogicalType) -> u32 {
    match ty.kind {
        LogicalKind::Char(n) | LogicalKind::Varchar(n) => n,
        _ => 0,
    }
}

/// The base64 Arrow IPC schema message of a one-field schema of `ty`, the form the engine's typed
/// NULL literal decodes (`encoded_field_type`).
fn typed_null_schema(ty: &LogicalType) -> PlanResult<String> {
    use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    use base64::Engine;
    let field: ArrowField = to_arrow_field("value", ty)
        .map_err(|e| PlanError::not_yet(format!("native expression: NULL of type {ty}: {e}")))?;
    let schema = Schema::new(vec![Arc::new(field)]);
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        &schema,
        &mut DictionaryTracker::new(false),
        &IpcWriteOptions::default(),
    );
    let mut bytes = vec![0xff, 0xff, 0xff, 0xff];
    bytes.extend_from_slice(&(encoded.ipc_message.len() as i32).to_le_bytes());
    bytes.extend_from_slice(&encoded.ipc_message);
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Encodes a Calc: the projections and an optional filter over one input.
pub fn encode_calc(
    projections: &[ScalarExpr],
    names: &[String],
    condition: Option<&ScalarExpr>,
) -> PlanResult<CalcSpec> {
    let mut enc = Encoder::new();
    let condition_root = match condition {
        Some(c) => enc.root(c)? as i64,
        None => -1,
    };
    let mut projection_roots = vec![];
    for p in projections {
        projection_roots.push(enc.root(p)?);
    }
    Ok(CalcSpec {
        expr: enc.finish(),
        projection_roots,
        condition_root,
        output_names: names.to_vec(),
    })
}

/// Encodes one predicate (a join's non-equi condition).
pub fn encode_predicate(e: &ScalarExpr) -> PlanResult<EncodedExpr> {
    let mut enc = Encoder::new();
    enc.root(e)?;
    Ok(enc.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bigint(i: usize) -> ScalarExpr {
        ScalarExpr::input(i, LogicalType::bigint())
    }

    #[test]
    fn decimal_times_bigint_uses_the_fused_kernel() {
        // q1: 0.908 * price, DECIMAL(4, 3) * BIGINT → DECIMAL(24, 3).
        let lit = ScalarExpr::literal(
            Literal::Decimal {
                unscaled: 908,
                scale: 3,
            },
            LogicalType::decimal(4, 3).not_null(),
        );
        let e = ScalarExpr::call(Op::Times, vec![lit, bigint(2)], LogicalType::decimal(24, 3));
        let spec = encode_calc(&[e], &["price".into()], None).unwrap();
        assert_eq!(
            spec.expr.kinds,
            vec![KIND_DECIMAL_MULTIPLY, KIND_LIT_DECIMAL, KIND_INPUT_REF]
        );
        assert_eq!(spec.expr.payload[0], 2403);
        assert_eq!(spec.expr.strings, vec![Some("908|4|3".into())]);
    }

    #[test]
    fn int_result_arithmetic_is_narrowed() {
        // MOD(BIGINT, INT) is INT in Flink: the native % runs in BIGINT and narrows.
        let e = ScalarExpr::call(
            Op::Mod,
            vec![
                bigint(0),
                ScalarExpr::literal(Literal::Integer(123), LogicalType::int().not_null()),
            ],
            LogicalType::int(),
        );
        let spec = encode_calc(&[], &[], Some(&e)).unwrap();
        assert_eq!(
            spec.expr.kinds,
            vec![KIND_CAST_NARROW, KIND_CALL, KIND_INPUT_REF, KIND_LIT_INT]
        );
        assert_eq!(spec.condition_root, 0);
    }

    #[test]
    fn n_ary_and_is_left_deep() {
        let b = |i| ScalarExpr::input(i, LogicalType::boolean());
        let e = ScalarExpr::call(Op::And, vec![b(0), b(1), b(2)], LogicalType::boolean());
        let enc = encode_predicate(&e).unwrap();
        assert_eq!(
            enc.kinds,
            vec![
                KIND_CALL,
                KIND_CALL,
                KIND_INPUT_REF,
                KIND_INPUT_REF,
                KIND_INPUT_REF
            ]
        );
        assert_eq!(enc.child_counts, vec![2, 2, 0, 0, 0]);
    }

    #[test]
    fn decimal_literals_round_half_up_or_overflow_to_null() {
        let mut enc = Encoder::new();
        enc.decimal_literal(12345, 3, 5, 2);
        assert_eq!(enc.out.strings, vec![Some("1235|5|2".into())]);
        let mut enc = Encoder::new();
        enc.decimal_literal(123_456, 0, 3, 0);
        assert_eq!(enc.out.kinds, vec![KIND_CAST_DECIMAL, KIND_LIT_NULL]);
    }

    #[test]
    fn unsupported_functions_are_named() {
        let e = ScalarExpr::call(
            Op::Function("JSON_QUERY".into()),
            vec![],
            LogicalType::string(),
        );
        let err = encode_predicate(&e).unwrap_err();
        assert!(err.to_string().contains("function JSON_QUERY"), "{err}");
    }
}
