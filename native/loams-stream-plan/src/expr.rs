//! Typed scalar expressions over a relation's fields: the planner's counterpart of Calcite's
//! `RexNode` (FC1 Task 10).
//!
//! The converter ([`crate::convert`]) builds them from the parsed SQL with the binder's types;
//! the encoder ([`crate::encode`]) writes them in the engine's prefix encoding.

use loams_stream_sql::types::{LogicalKind, LogicalType, TimestampKind};

/// A literal value. Types travel beside it on [`ScalarExpr::Literal`].
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    /// Any exact integer (`TINYINT` to `BIGINT`).
    Integer(i64),
    /// An exact decimal: the unscaled value and the scale; the precision is the type's.
    Decimal {
        unscaled: i128,
        scale: u8,
    },
    /// `FLOAT` or `DOUBLE`.
    Double(f64),
    /// A character string.
    String(String),
    /// A binary string.
    Binary(Vec<u8>),
    /// `INTERVAL ... YEAR TO MONTH` in months.
    Months(i32),
    /// `INTERVAL ... DAY TO SECOND` in milliseconds.
    Millis(i64),
    /// `DATE` in days since the epoch.
    Date(i32),
    /// `TIME` in milliseconds of the day.
    Time(i32),
    /// `TIMESTAMP`/`TIMESTAMP_LTZ`: epoch milliseconds and the nanoseconds within the millisecond.
    Timestamp {
        millis: i64,
        nano_of_milli: i32,
    },
    /// A symbol argument (`EXTRACT`'s unit, `TRIM`'s side, a `JSON` clause).
    Symbol(String),
}

/// An operator: Calcite's `SqlKind` for the operators with special syntax, and a function name for
/// the rest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Op {
    Plus,
    Minus,
    Times,
    Divide,
    Mod,
    Negate,
    Eq,
    NotEq,
    Gt,
    Ge,
    Lt,
    Le,
    IsDistinctFrom,
    IsNotDistinctFrom,
    And,
    Or,
    Not,
    IsNull,
    IsNotNull,
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    /// Searched `CASE`: `[when, then]*, else`.
    Case,
    Like,
    SimilarTo,
    /// `CAST` (and `TRY_CAST` when `true`) to the call's type.
    Cast {
        try_cast: bool,
    },
    /// Re-types a value without changing it (strips a time attribute).
    Reinterpret,
    /// `row.field`.
    FieldAccess(String),
    /// `array[i]`, `map[key]`.
    Item,
    /// `PROCTIME()`.
    Proctime,
    /// Any other built-in function, by its upper-case name.
    Function(String),
}

/// A typed scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarExpr {
    /// The input field at `index`.
    Input {
        index: usize,
        ty: LogicalType,
    },
    Literal {
        value: Literal,
        ty: LogicalType,
    },
    Call {
        op: Op,
        args: Vec<ScalarExpr>,
        ty: LogicalType,
    },
}

impl ScalarExpr {
    pub fn input(index: usize, ty: LogicalType) -> Self {
        ScalarExpr::Input { index, ty }
    }

    pub fn literal(value: Literal, ty: LogicalType) -> Self {
        ScalarExpr::Literal { value, ty }
    }

    pub fn call(op: Op, args: Vec<ScalarExpr>, ty: LogicalType) -> Self {
        ScalarExpr::Call { op, args, ty }
    }

    pub fn bool(value: bool) -> Self {
        ScalarExpr::literal(Literal::Boolean(value), LogicalType::boolean().not_null())
    }

    pub fn ty(&self) -> &LogicalType {
        match self {
            ScalarExpr::Input { ty, .. }
            | ScalarExpr::Literal { ty, .. }
            | ScalarExpr::Call { ty, .. } => ty,
        }
    }

    /// The input index if this is a plain input reference.
    pub fn as_input(&self) -> Option<usize> {
        match self {
            ScalarExpr::Input { index, .. } => Some(*index),
            _ => None,
        }
    }

    /// Every input index the expression reads.
    pub fn inputs(&self, out: &mut Vec<usize>) {
        match self {
            ScalarExpr::Input { index, .. } => {
                if !out.contains(index) {
                    out.push(*index)
                }
            }
            ScalarExpr::Literal { .. } => {}
            ScalarExpr::Call { args, .. } => args.iter().for_each(|a| a.inputs(out)),
        }
    }

    /// Rewrites input references with `f`.
    pub fn map_inputs(&self, f: &impl Fn(usize) -> usize) -> ScalarExpr {
        match self {
            ScalarExpr::Input { index, ty } => ScalarExpr::input(f(*index), ty.clone()),
            ScalarExpr::Literal { .. } => self.clone(),
            ScalarExpr::Call { op, args, ty } => ScalarExpr::call(
                op.clone(),
                args.iter().map(|a| a.map_inputs(f)).collect(),
                ty.clone(),
            ),
        }
    }

    /// Replaces input references by the expressions they stand for (inlining a projection).
    pub fn substitute(&self, exprs: &[ScalarExpr]) -> ScalarExpr {
        match self {
            ScalarExpr::Input { index, .. } => exprs[*index].clone(),
            ScalarExpr::Literal { .. } => self.clone(),
            ScalarExpr::Call { op, args, ty } => ScalarExpr::call(
                op.clone(),
                args.iter().map(|a| a.substitute(exprs)).collect(),
                ty.clone(),
            ),
        }
    }

    /// Whether the expression's value can differ between two evaluations on the same row.
    pub fn is_deterministic(&self) -> bool {
        match self {
            ScalarExpr::Input { .. } | ScalarExpr::Literal { .. } => true,
            ScalarExpr::Call { op, args, .. } => {
                let own = match op {
                    Op::Proctime => false,
                    Op::Function(name) => !matches!(
                        name.as_str(),
                        "RAND"
                            | "RAND_INTEGER"
                            | "UUID"
                            | "NOW"
                            | "CURRENT_TIMESTAMP"
                            | "LOCALTIMESTAMP"
                            | "CURRENT_ROW_TIMESTAMP"
                            | "UNIX_TIMESTAMP"
                            | "CURRENT_DATE"
                            | "CURRENT_TIME"
                            | "LOCALTIME"
                    ),
                    _ => true,
                };
                own && args.iter().all(ScalarExpr::is_deterministic)
            }
        }
    }

    /// Splits a conjunction into its terms.
    pub fn conjuncts(self) -> Vec<ScalarExpr> {
        match self {
            ScalarExpr::Call {
                op: Op::And, args, ..
            } => args.into_iter().flat_map(ScalarExpr::conjuncts).collect(),
            other => vec![other],
        }
    }

    /// The conjunction of `terms` (`TRUE` for none).
    pub fn and_all(mut terms: Vec<ScalarExpr>) -> ScalarExpr {
        match terms.len() {
            0 => ScalarExpr::bool(true),
            1 => terms.remove(0),
            _ => {
                let nullable = terms.iter().any(|t| t.ty().nullable);
                ScalarExpr::call(
                    Op::And,
                    terms,
                    LogicalType::boolean().with_nullable(nullable),
                )
            }
        }
    }
}

/// Whether `ty` is a rowtime attribute.
pub fn is_rowtime(ty: &LogicalType) -> bool {
    matches!(
        ty.kind,
        LogicalKind::Timestamp {
            kind: TimestampKind::Rowtime,
            ..
        } | LogicalKind::TimestampLtz {
            kind: TimestampKind::Rowtime,
            ..
        }
    )
}

/// Whether `ty` is a processing-time attribute.
pub fn is_proctime(ty: &LogicalType) -> bool {
    matches!(
        ty.kind,
        LogicalKind::Timestamp {
            kind: TimestampKind::Proctime,
            ..
        } | LogicalKind::TimestampLtz {
            kind: TimestampKind::Proctime,
            ..
        }
    )
}

/// `ty` without its time-attribute marker.
pub fn materialized(ty: &LogicalType) -> LogicalType {
    let kind = match &ty.kind {
        LogicalKind::Timestamp { precision, .. } => LogicalKind::Timestamp {
            precision: *precision,
            kind: TimestampKind::Regular,
        },
        LogicalKind::TimestampLtz { precision, .. } => LogicalKind::TimestampLtz {
            precision: *precision,
            kind: TimestampKind::Regular,
        },
        other => other.clone(),
    };
    LogicalType {
        kind,
        nullable: ty.nullable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conjuncts_flatten_nested_ands() {
        let a = ScalarExpr::input(0, LogicalType::boolean());
        let b = ScalarExpr::input(1, LogicalType::boolean());
        let c = ScalarExpr::input(2, LogicalType::boolean());
        let inner = ScalarExpr::and_all(vec![a.clone(), b.clone()]);
        let outer = ScalarExpr::and_all(vec![inner, c.clone()]);
        assert_eq!(outer.conjuncts(), vec![a, b, c]);
    }

    #[test]
    fn substitute_inlines_a_projection() {
        let proj = vec![
            ScalarExpr::literal(Literal::Integer(7), LogicalType::int()),
            ScalarExpr::input(4, LogicalType::bigint()),
        ];
        let e = ScalarExpr::call(
            Op::Plus,
            vec![
                ScalarExpr::input(1, LogicalType::bigint()),
                ScalarExpr::input(0, LogicalType::int()),
            ],
            LogicalType::bigint(),
        );
        let mut inputs = vec![];
        e.substitute(&proj).inputs(&mut inputs);
        assert_eq!(inputs, vec![4]);
    }
}
