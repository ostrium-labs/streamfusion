//! `EXPLAIN` in Flink's shape: the optimized physical plan as an indented tree of ExecNode names
//! with their attributes and, for `EXPLAIN CHANGELOG_MODE`, each node's changelog mode (FC1
//! Task 10).

use loams_stream_sql::types::LogicalType;

use crate::expr::{Literal, Op, ScalarExpr};
use crate::logical::Field;
use crate::physical::{NodeId, PhysicalOp, PhysicalPlan};

/// Renders an expression the way Flink's plans print `RexNode`s: `+(a, b)`, `=(x, 1)`.
pub fn render_expr(e: &ScalarExpr, fields: &[Field]) -> String {
    match e {
        ScalarExpr::Input { index, .. } => fields
            .get(*index)
            .map(|f| f.name.clone())
            .unwrap_or_else(|| format!("${index}")),
        ScalarExpr::Literal { value, ty } => render_literal(value, ty),
        ScalarExpr::Call { op, args, ty } => {
            let a: Vec<String> = args.iter().map(|x| render_expr(x, fields)).collect();
            let name = match op {
                Op::Plus => "+",
                Op::Minus => "-",
                Op::Times => "*",
                Op::Divide => "/",
                Op::Mod => "MOD",
                Op::Negate => "-",
                Op::Eq => "=",
                Op::NotEq => "<>",
                Op::Gt => ">",
                Op::Ge => ">=",
                Op::Lt => "<",
                Op::Le => "<=",
                Op::IsDistinctFrom => "IS DISTINCT FROM",
                Op::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
                Op::And => "AND",
                Op::Or => "OR",
                Op::Not => "NOT",
                Op::IsNull => "IS NULL",
                Op::IsNotNull => "IS NOT NULL",
                Op::IsTrue => "IS TRUE",
                Op::IsNotTrue => "IS NOT TRUE",
                Op::IsFalse => "IS FALSE",
                Op::IsNotFalse => "IS NOT FALSE",
                Op::Case => "CASE",
                Op::Like => "LIKE",
                Op::SimilarTo => "SIMILAR TO",
                Op::Cast { try_cast: false } => {
                    return format!("CAST({} AS {})", a.join(", "), ty);
                }
                Op::Cast { try_cast: true } => {
                    return format!("TRY_CAST({} AS {})", a.join(", "), ty);
                }
                Op::Reinterpret => return a.join(", "),
                Op::FieldAccess(name) => return format!("{}.{name}", a.join(", ")),
                Op::Item => "ITEM",
                Op::Proctime => "PROCTIME",
                Op::Function(name) => name.as_str(),
            };
            format!("{name}({})", a.join(", "))
        }
    }
}

fn render_literal(value: &Literal, ty: &LogicalType) -> String {
    match value {
        Literal::Null => format!("null:{ty}"),
        Literal::Boolean(b) => b.to_string(),
        Literal::Integer(v) => v.to_string(),
        Literal::Decimal { unscaled, scale } => {
            let s = unscaled.unsigned_abs().to_string();
            let scale = *scale as usize;
            let sign = if *unscaled < 0 { "-" } else { "" };
            if scale == 0 {
                format!("{sign}{s}")
            } else {
                let padded = format!("{s:0>width$}", width = scale + 1);
                let (w, f) = padded.split_at(padded.len() - scale);
                format!("{sign}{w}.{f}")
            }
        }
        Literal::Double(v) => format!("{v:E}"),
        Literal::String(s) => format!("_UTF-16LE'{s}'"),
        Literal::Binary(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        ),
        Literal::Months(m) => format!("{m}:{ty}"),
        Literal::Millis(ms) => format!("{ms}:{ty}"),
        Literal::Date(d) => format!("{d}:DATE"),
        Literal::Time(t) => format!("{t}:TIME"),
        Literal::Timestamp { millis, .. } => format!("{millis}:{ty}"),
        Literal::Symbol(s) => format!("FLAG({s})"),
    }
}

/// The optimized physical plan as Flink prints it; with `changelog`, each node also shows its
/// `changelogMode` (`EXPLAIN CHANGELOG_MODE`).
pub fn explain(plan: &PhysicalPlan, changelog: bool) -> String {
    let mut out = String::new();
    for sink in plan.sinks() {
        tree(plan, sink, changelog, "", true, &mut out);
    }
    out
}

fn tree(
    plan: &PhysicalPlan,
    id: NodeId,
    changelog: bool,
    prefix: &str,
    root: bool,
    out: &mut String,
) {
    let node = plan.node(id);
    let mut attrs = node.description.clone();
    if changelog {
        let mode = if matches!(node.op, PhysicalOp::Sink(_)) {
            "[NONE]".to_owned()
        } else {
            node.changelog.to_string()
        };
        if !attrs.is_empty() {
            attrs.push_str(", ");
        }
        attrs.push_str(&format!("changelogMode={mode}"));
    }
    if root {
        out.push_str(&format!("{}({attrs})\n", node.name));
    } else {
        out.push_str(&format!("{prefix}+- {}({attrs})\n", node.name));
    }
    let child_prefix = if root {
        String::new()
    } else {
        format!("{prefix}   ")
    };
    for edge in &node.inputs {
        tree(plan, edge.node, changelog, &child_prefix, false, out);
    }
}
