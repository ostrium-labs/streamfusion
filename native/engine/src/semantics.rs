//! Flink value semantics for Loams Stream's SQL front end (FC1 Task 4, design 82 §7.1).
//!
//! `loams-stream-sql` derives Flink's result types (`LogicalTypeMerging`); the values themselves
//! come from the kernels the engine already runs, so the two cannot drift. This module exposes the
//! minimum the binder's tests and the planner need, as DataFusion scalar functions bound to a
//! Flink-resolved result type. Behind the `semantics` feature; the JNI path is unchanged.

use datafusion::logical_expr::ScalarUDF;

use crate::expr::DecimalDivide;
use crate::flink_functions::decimal::{DecimalBinary, DecimalCast, DecimalOp, DecimalRound};

/// Flink's binary decimal operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalArithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// The kernel for `left <op> right` with the result type `DECIMAL(precision, scale)` that Flink's
/// planner derived (`flink_decimal_add`, ..., `flink_decimal_mod`). Operands are `Decimal128` or
/// integer columns; an out-of-range result is NULL, as in Flink's `DecimalDataUtils`.
pub fn decimal_arithmetic(op: DecimalArithmetic, precision: u8, scale: i8) -> ScalarUDF {
    match op {
        DecimalArithmetic::Add => {
            ScalarUDF::new_from_impl(DecimalBinary::new(DecimalOp::Add, precision, scale))
        }
        DecimalArithmetic::Subtract => {
            ScalarUDF::new_from_impl(DecimalBinary::new(DecimalOp::Subtract, precision, scale))
        }
        DecimalArithmetic::Multiply => {
            ScalarUDF::new_from_impl(DecimalBinary::new(DecimalOp::Multiply, precision, scale))
        }
        DecimalArithmetic::Divide => {
            ScalarUDF::new_from_impl(DecimalDivide::new(precision, scale, false))
        }
        DecimalArithmetic::Modulo => {
            ScalarUDF::new_from_impl(DecimalDivide::new(precision, scale, true))
        }
    }
}

/// `CAST(x AS DECIMAL(precision, scale))` from a decimal or integer, rounding HALF_UP and
/// answering NULL on overflow (`flink_decimal_cast`).
pub fn decimal_cast(precision: u8, scale: i8) -> ScalarUDF {
    ScalarUDF::new_from_impl(DecimalCast::new(precision, scale))
}

/// `ROUND(x, n)` (or `TRUNCATE(x, n)` when `truncate`) with the result type Flink derived
/// (`LogicalTypeMerging.findRoundDecimalType`).
pub fn decimal_round(precision: u8, scale: i8, truncate: bool) -> ScalarUDF {
    if truncate {
        ScalarUDF::new_from_impl(DecimalRound::truncate(precision, scale))
    } else {
        ScalarUDF::new_from_impl(DecimalRound::new(precision, scale))
    }
}
