//! Query and expression typing with Flink's rules.

use sqlparser::ast::{
    AccessExpr, BinaryOperator, CeilFloorKind, DataType, DateTimeField, Expr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Join, JoinConstraint,
    JoinOperator, ObjectName, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    SetOperator, Spanned, Subscript, TableAlias, TableFactor, TableWithJoins, UnaryOperator, Value,
};
use sqlparser::tokenizer::Span;

use super::catalog::ResolvedColumn;
use super::functions;
use super::{BindError, BindResult, Binder, OutputColumn, name_parts};
use crate::dialect::{map_constructor, table_argument};
use crate::error::Feature;
use crate::types::casts::cast_allowed as cast_allowed_sql;
use crate::types::merging::{
    Decimal, addition_decimal, as_decimal, division_decimal, find_common_type, modulo_decimal,
    multiplication_decimal,
};
use crate::types::{
    LogicalKind, LogicalRowField, LogicalType, LogicalTypeFamily, TimestampKind, type_of_cast,
};

/// A field visible in a scope, with the table alias that qualifies it.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopeField {
    pub qualifier: Option<String>,
    pub name: String,
    pub ty: LogicalType,
}

/// The fields a query block's expressions can reference.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Scope {
    pub fields: Vec<ScopeField>,
    /// Common table expressions in scope: name and output columns.
    pub ctes: Vec<(String, Vec<OutputColumn>)>,
}

impl Scope {
    pub fn from_columns(qualifier: Option<&str>, columns: &[ResolvedColumn]) -> Self {
        Scope {
            fields: columns
                .iter()
                .map(|c| ScopeField {
                    qualifier: qualifier.map(str::to_owned),
                    name: c.name.clone(),
                    ty: c.ty.clone(),
                })
                .collect(),
            ctes: vec![],
        }
    }

    fn resolve(&self, qualifier: Option<&str>, name: &str, span: Span) -> BindResult<LogicalType> {
        let matches: Vec<&ScopeField> = self
            .fields
            .iter()
            .filter(|f| {
                f.name == name && qualifier.is_none_or(|q| f.qualifier.as_deref() == Some(q))
            })
            .collect();
        match matches.as_slice() {
            [f] => Ok(f.ty.clone()),
            [] => {
                // Flink's identifiers are case-sensitive, but Calcite reports a case mismatch.
                Err(BindError::validation(
                    match qualifier {
                        Some(q) => format!("Column '{q}.{name}' not found in any table"),
                        None => format!("Column '{name}' not found in any table"),
                    },
                    span,
                ))
            }
            [first, ..] => {
                // Join keys duplicated on both sides with the same type are ambiguous in Flink,
                // but only when referenced unqualified.
                if matches.iter().all(|f| f.ty == first.ty) && qualifier.is_some() {
                    Ok(first.ty.clone())
                } else {
                    Err(BindError::validation(
                        format!("Column '{name}' is ambiguous"),
                        span,
                    ))
                }
            }
        }
    }
}

/// A time attribute version of `ty` for a watermark column, if `ty` can be one.
pub fn as_rowtime(ty: &LogicalType) -> Option<LogicalType> {
    let kind = match &ty.kind {
        LogicalKind::Timestamp { precision, .. } if *precision <= 3 => LogicalKind::Timestamp {
            precision: *precision,
            kind: TimestampKind::Rowtime,
        },
        LogicalKind::TimestampLtz { precision, .. } if *precision <= 3 => {
            LogicalKind::TimestampLtz {
                precision: *precision,
                kind: TimestampKind::Rowtime,
            }
        }
        _ => return None,
    };
    Some(LogicalType {
        kind,
        nullable: ty.nullable,
    })
}

/// A plain (non time attribute) copy of `ty`: what an expression over a time attribute yields.
pub(crate) fn materialize(ty: &LogicalType) -> LogicalType {
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

/// Binds a query and returns its output columns.
pub fn bind_query(b: &Binder, q: &Query) -> BindResult<Vec<OutputColumn>> {
    bind_query_in(b, q, &Scope::default())
}

fn bind_query_in(b: &Binder, q: &Query, outer: &Scope) -> BindResult<Vec<OutputColumn>> {
    let mut ctx = Scope {
        fields: outer.fields.clone(),
        ctes: outer.ctes.clone(),
    };
    if let Some(with) = &q.with {
        for cte in &with.cte_tables {
            let mut out = bind_query_in(b, &cte.query, &ctx)?;
            for (i, c) in cte.alias.columns.iter().enumerate() {
                if let Some(o) = out.get_mut(i) {
                    o.name = c.name.value.clone();
                }
            }
            ctx.ctes.push((cte.alias.name.value.clone(), out));
        }
    }
    bind_set_expr(b, &q.body, &ctx)
}

fn bind_set_expr(b: &Binder, body: &SetExpr, ctx: &Scope) -> BindResult<Vec<OutputColumn>> {
    match body {
        SetExpr::Select(select) => bind_select(b, select, ctx),
        SetExpr::Query(q) => bind_query_in(b, q, ctx),
        SetExpr::SetOperation {
            left, right, op, ..
        } => {
            let l = bind_set_expr(b, left, ctx)?;
            let r = bind_set_expr(b, right, ctx)?;
            if l.len() != r.len() {
                let kind = match op {
                    SetOperator::Union => "UNION",
                    SetOperator::Except => "EXCEPT",
                    SetOperator::Intersect => "INTERSECT",
                    _ => "set operation",
                };
                return Err(BindError::validation(
                    format!("Column count mismatch in {kind}"),
                    body.span(),
                ));
            }
            l.into_iter()
                .zip(r)
                .map(|(a, c)| {
                    let ty = find_common_type(&[materialize(&a.ty), materialize(&c.ty)])
                        .unwrap_or(a.ty.clone());
                    Ok(OutputColumn { name: a.name, ty })
                })
                .collect()
        }
        SetExpr::Values(values) => {
            let mut columns: Vec<Vec<LogicalType>> = vec![];
            for row in &values.rows {
                for (i, e) in row.iter().enumerate() {
                    let ty = type_expr_in(b, ctx, e)?;
                    if columns.len() <= i {
                        columns.push(vec![]);
                    }
                    columns[i].push(ty);
                }
            }
            Ok(columns
                .into_iter()
                .enumerate()
                .map(|(i, tys)| OutputColumn {
                    name: format!("EXPR${i}"),
                    ty: find_common_type(&tys).unwrap_or_else(|| tys[0].clone()),
                })
                .collect())
        }
        SetExpr::Table(t) => {
            let name = t.table_name.clone().unwrap_or_default();
            let (_, schema) =
                b.resolve_table(&ObjectName::from(vec![sqlparser::ast::Ident::new(name)]))?;
            Ok(schema
                .columns
                .into_iter()
                .map(|c| OutputColumn {
                    name: c.name,
                    ty: c.ty,
                })
                .collect())
        }
        other => Err(BindError::validation(
            format!("Unsupported query form: {other}"),
            other.span(),
        )),
    }
}

fn bind_select(b: &Binder, select: &Select, ctx: &Scope) -> BindResult<Vec<OutputColumn>> {
    let mut scope = Scope {
        fields: vec![],
        ctes: ctx.ctes.clone(),
    };
    for twj in &select.from {
        let fields = bind_table_with_joins(b, twj, ctx)?;
        scope.fields.extend(fields);
    }
    // Correlated references see the outer scope after the local one.
    let mut lookup = scope.clone();
    lookup.fields.extend(ctx.fields.iter().cloned());

    if let Some(w) = &select.selection {
        type_expr_in(b, &lookup, w)?;
    }
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for e in exprs {
            type_group_item(b, &lookup, e)?;
        }
    }
    let mut out = vec![];
    for (i, item) in select.projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let ty = type_expr_in(b, &lookup, e)?;
                out.push(OutputColumn {
                    name: default_name(e).unwrap_or_else(|| format!("EXPR${i}")),
                    ty,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let ty = type_expr_in(b, &lookup, expr)?;
                out.push(OutputColumn {
                    name: alias.value.clone(),
                    ty,
                });
            }
            SelectItem::Wildcard(_) => {
                if scope.fields.is_empty() {
                    return Err(BindError::validation(
                        "SELECT * not allowed in query with no FROM clause",
                        item.span(),
                    ));
                }
                out.extend(scope.fields.iter().map(|f| OutputColumn {
                    name: f.name.clone(),
                    ty: f.ty.clone(),
                }));
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let q = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(n) => {
                        name_parts(n).last().cloned().unwrap_or_default()
                    }
                    SelectItemQualifiedWildcardKind::Expr(e) => e.to_string(),
                };
                let fields: Vec<_> = scope
                    .fields
                    .iter()
                    .filter(|f| f.qualifier.as_deref() == Some(q.as_str()))
                    .collect();
                if fields.is_empty() {
                    return Err(BindError::validation(
                        format!("Unknown identifier '{q}'"),
                        item.span(),
                    ));
                }
                out.extend(fields.into_iter().map(|f| OutputColumn {
                    name: f.name.clone(),
                    ty: f.ty.clone(),
                }));
            }
            SelectItem::ExprWithAliases { expr, aliases } => {
                let ty = type_expr_in(b, &lookup, expr)?;
                for a in aliases {
                    out.push(OutputColumn {
                        name: a.value.clone(),
                        ty: ty.clone(),
                    });
                }
            }
        }
    }
    if let Some(h) = &select.having {
        // HAVING may use output aliases in Flink (Calcite's conformance allows it).
        let mut having_scope = lookup.clone();
        having_scope.fields.extend(out.iter().map(|o| ScopeField {
            qualifier: None,
            name: o.name.clone(),
            ty: o.ty.clone(),
        }));
        type_expr_in(b, &having_scope, h).or_else(|e| {
            if e.feature.is_some() {
                Err(e)
            } else {
                Ok(LogicalType::boolean())
            }
        })?;
    }
    // Duplicate output names are made unique as Calcite does (`name0`).
    let mut seen = std::collections::BTreeMap::<String, usize>::new();
    for c in out.iter_mut() {
        let n = seen.entry(c.name.clone()).or_insert(0);
        if *n > 0 {
            c.name = format!("{}{}", c.name, *n - 1);
        }
        *n += 1;
    }
    Ok(out)
}

fn type_group_item(b: &Binder, scope: &Scope, e: &Expr) -> BindResult<()> {
    match e {
        Expr::GroupingSets(sets) | Expr::Cube(sets) | Expr::Rollup(sets) => {
            for set in sets {
                for e in set {
                    type_expr_in(b, scope, e)?;
                }
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for e in items {
                type_expr_in(b, scope, e)?;
            }
            Ok(())
        }
        // GROUP BY ordinals and output aliases are not Flink's default conformance; positional
        // and alias forms fall through to name resolution.
        _ => type_expr_in(b, scope, e).map(|_| ()),
    }
}

fn default_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        Expr::Nested(inner) => default_name(inner),
        _ => None,
    }
}

fn bind_table_with_joins(
    b: &Binder,
    twj: &TableWithJoins,
    ctx: &Scope,
) -> BindResult<Vec<ScopeField>> {
    let mut fields = bind_table_factor(b, &twj.relation, ctx, &[])?;
    for join in &twj.joins {
        fields = bind_join(b, fields, join, ctx)?;
    }
    Ok(fields)
}

fn bind_join(
    b: &Binder,
    left: Vec<ScopeField>,
    join: &Join,
    ctx: &Scope,
) -> BindResult<Vec<ScopeField>> {
    let mut right = bind_table_factor(b, &join.relation, ctx, &left)?;
    let mut left = left;
    let (constraint, left_null, right_null) = match &join.join_operator {
        JoinOperator::Join(c) | JoinOperator::Inner(c) => (Some(c), false, false),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (Some(c), false, true),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (Some(c), true, false),
        JoinOperator::FullOuter(c) => (Some(c), true, true),
        JoinOperator::CrossJoin(c) => (Some(c), false, false),
        JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c) => (Some(c), false, false),
        _ => {
            return Err(BindError::validation(
                format!("Unsupported join type: {join}"),
                join.span(),
            ));
        }
    };
    if left_null {
        for f in left.iter_mut() {
            f.ty = f.ty.clone().nullable();
        }
    }
    if right_null {
        for f in right.iter_mut() {
            f.ty = f.ty.clone().nullable();
        }
    }
    let mut all = left;
    all.extend(right);
    let scope = Scope {
        fields: all.clone(),
        ctes: ctx.ctes.clone(),
    };
    match constraint {
        Some(JoinConstraint::On(e)) => {
            let mut lookup = scope.clone();
            lookup.fields.extend(ctx.fields.iter().cloned());
            type_expr_in(b, &lookup, e)?;
        }
        Some(JoinConstraint::Using(cols)) => {
            for c in cols {
                let name = name_parts(c).last().cloned().unwrap_or_default();
                if !all.iter().any(|f| f.name == name) {
                    return Err(BindError::validation(
                        format!("Column '{name}' not found in any table"),
                        c.span(),
                    ));
                }
            }
        }
        _ => {}
    }
    Ok(all)
}

fn alias_fields(
    mut fields: Vec<ScopeField>,
    alias: Option<&TableAlias>,
    default: Option<String>,
) -> Vec<ScopeField> {
    let qualifier = alias.map(|a| a.name.value.clone()).or(default);
    for f in fields.iter_mut() {
        f.qualifier = qualifier.clone();
    }
    if let Some(a) = alias {
        for (i, c) in a.columns.iter().enumerate() {
            if let Some(f) = fields.get_mut(i) {
                f.name = c.name.value.clone();
            }
        }
    }
    fields
}

fn bind_table_factor(
    b: &Binder,
    factor: &TableFactor,
    ctx: &Scope,
    left: &[ScopeField],
) -> BindResult<Vec<ScopeField>> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            version,
            ..
        } => {
            if args.is_some() {
                return Err(BindError::unsupported(
                    Feature::LATERAL_TABLE_FUNCTIONS,
                    factor.span(),
                ));
            }
            let parts = name_parts(name);
            let fields: Vec<ScopeField> = if parts.len() == 1
                && let Some((_, cols)) = ctx.ctes.iter().rev().find(|(n, _)| *n == parts[0])
            {
                cols.iter()
                    .map(|c| ScopeField {
                        qualifier: None,
                        name: c.name.clone(),
                        ty: c.ty.clone(),
                    })
                    .collect()
            } else {
                let (_, schema) = b.resolve_table(name)?;
                schema
                    .columns
                    .iter()
                    .map(|c| ScopeField {
                        qualifier: None,
                        name: c.name.clone(),
                        ty: c.ty.clone(),
                    })
                    .collect()
            };
            if let Some(sqlparser::ast::TableVersion::ForSystemTimeAsOf(e)) = version {
                let scope = Scope {
                    fields: left.to_vec(),
                    ctes: ctx.ctes.clone(),
                };
                type_expr_in(b, &scope, e)?;
            }
            Ok(alias_fields(fields, alias.as_ref(), parts.last().cloned()))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let out = bind_query_in(b, subquery, ctx)?;
            let fields = out
                .into_iter()
                .map(|c| ScopeField {
                    qualifier: None,
                    name: c.name,
                    ty: c.ty,
                })
                .collect();
            Ok(alias_fields(fields, alias.as_ref(), None))
        }
        TableFactor::TableFunction { expr, alias } => {
            let fields = bind_table_function(b, expr, ctx, left)?;
            Ok(alias_fields(fields, alias.as_ref(), None))
        }
        TableFactor::Function { .. } => Err(BindError::unsupported(
            Feature::LATERAL_TABLE_FUNCTIONS,
            factor.span(),
        )),
        TableFactor::UNNEST {
            alias,
            array_exprs,
            with_offset,
            ..
        } => {
            let mut lookup = Scope {
                fields: left.to_vec(),
                ctes: ctx.ctes.clone(),
            };
            lookup.fields.extend(ctx.fields.iter().cloned());
            let mut fields = vec![];
            for (i, e) in array_exprs.iter().enumerate() {
                let ty = type_expr_in(b, &lookup, e)?;
                match &ty.kind {
                    LogicalKind::Array(el) | LogicalKind::Multiset(el) => match &el.kind {
                        LogicalKind::Row(row) => fields.extend(row.iter().map(|f| ScopeField {
                            qualifier: None,
                            name: f.name.clone(),
                            ty: f.ty.clone(),
                        })),
                        _ => fields.push(ScopeField {
                            qualifier: None,
                            name: format!("EXPR${i}"),
                            ty: (**el).clone(),
                        }),
                    },
                    LogicalKind::Map(k, v) => {
                        fields.push(ScopeField {
                            qualifier: None,
                            name: "KEY".into(),
                            ty: (**k).clone(),
                        });
                        fields.push(ScopeField {
                            qualifier: None,
                            name: "VALUE".into(),
                            ty: (**v).clone(),
                        });
                    }
                    _ => {
                        return Err(BindError::validation(
                            format!("UNNEST argument must be a collection, but was {ty}"),
                            e.span(),
                        ));
                    }
                }
            }
            if *with_offset {
                fields.push(ScopeField {
                    qualifier: None,
                    name: "ORDINALITY".into(),
                    ty: LogicalType::int().not_null(),
                });
            }
            Ok(alias_fields(fields, alias.as_ref(), None))
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let fields = bind_table_with_joins(b, table_with_joins, ctx)?;
            Ok(match alias {
                Some(a) => alias_fields(fields, Some(a), None),
                None => fields,
            })
        }
        TableFactor::MatchRecognize { .. } => Err(BindError::unsupported(
            Feature::MATCH_RECOGNIZE,
            factor.span(),
        )),
        other => Err(BindError::validation(
            format!("Unsupported table reference: {other}"),
            other.span(),
        )),
    }
}

fn function_args(f: &Function) -> Vec<&FunctionArg> {
    match &f.args {
        FunctionArguments::List(list) => list.args.iter().collect(),
        _ => vec![],
    }
}

fn arg_expr(arg: &FunctionArg) -> Option<&Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        }
        | FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => Some(e),
        _ => None,
    }
}

fn arg_name(arg: &FunctionArg) -> Option<String> {
    match arg {
        FunctionArg::Named { name, .. } => Some(name.value.to_uppercase()),
        FunctionArg::ExprNamed { name, .. } => Some(name.to_string().to_uppercase()),
        _ => None,
    }
}

/// `TABLE(TUMBLE|HOP|CUMULATE|SESSION(TABLE t, DESCRIPTOR(ts), ...))`.
fn bind_table_function(
    b: &Binder,
    expr: &Expr,
    ctx: &Scope,
    left: &[ScopeField],
) -> BindResult<Vec<ScopeField>> {
    let Expr::Function(f) = expr else {
        return Err(BindError::unsupported(
            Feature::LATERAL_TABLE_FUNCTIONS,
            expr.span(),
        ));
    };
    let name = name_parts(&f.name).join(".").to_uppercase();
    if !matches!(name.as_str(), "TUMBLE" | "HOP" | "CUMULATE" | "SESSION") {
        if name == "ML_PREDICT" || name == "ML_EVALUATE" {
            return Err(BindError::unsupported(Feature::MODEL, expr.span()));
        }
        return Err(BindError::unsupported(
            Feature::LATERAL_TABLE_FUNCTIONS,
            expr.span(),
        ));
    }
    let args = function_args(f);
    let mut input: Option<Vec<ScopeField>> = None;
    let mut descriptor: Option<String> = None;
    let mut intervals = 0;
    for arg in &args {
        let Some(e) = arg_expr(arg) else { continue };
        let named = arg_name(arg);
        if let Some(t) = table_argument(e) {
            input = Some(bind_table_operand(b, t.table, ctx, left)?);
            continue;
        }
        if named.as_deref() == Some("DATA") {
            input = Some(bind_table_operand(b, e, ctx, left)?);
            continue;
        }
        if let Expr::Function(d) = e
            && name_parts(&d.name)
                .join(".")
                .eq_ignore_ascii_case("DESCRIPTOR")
        {
            if let Some(first) = function_args(d).first().and_then(|a| arg_expr(a)) {
                descriptor = default_name(first);
            }
            continue;
        }
        intervals += 1;
        let ty = type_expr_in(b, &Scope::default(), e)?;
        if !ty.is(LogicalTypeFamily::Interval) {
            return Err(BindError::validation(
                format!("{name} window size must be an interval, but was {ty}"),
                e.span(),
            ));
        }
    }
    let Some(mut fields) = input else {
        return Err(BindError::validation(
            format!("Missing table argument of {name}"),
            expr.span(),
        ));
    };
    let Some(col) = descriptor else {
        return Err(BindError::validation(
            format!("Missing DESCRIPTOR argument of {name}"),
            expr.span(),
        ));
    };
    let expected = match name.as_str() {
        "TUMBLE" | "SESSION" => 1,
        _ => 2,
    };
    if intervals < expected {
        return Err(BindError::validation(
            format!("Invalid number of arguments to function '{name}'"),
            expr.span(),
        ));
    }
    let Some(time) = fields.iter().find(|f| f.name == col).map(|f| f.ty.clone()) else {
        return Err(BindError::validation(
            format!("Column '{col}' not found in any table"),
            expr.span(),
        ));
    };
    let window_time = match &time.kind {
        LogicalKind::Timestamp { kind, precision } if *kind != TimestampKind::Regular => {
            LogicalType::new(LogicalKind::Timestamp {
                precision: *precision,
                kind: *kind,
            })
        }
        LogicalKind::TimestampLtz { kind, precision } if *kind != TimestampKind::Regular => {
            LogicalType::new(LogicalKind::TimestampLtz {
                precision: *precision,
                kind: *kind,
            })
        }
        _ => {
            return Err(BindError::validation(
                format!(
                    "The window function {name}(TABLE table_name, DESCRIPTOR(timecol), datetime interval) requires the timecol is a time attribute type, but is {time}."
                ),
                expr.span(),
            ));
        }
    };
    let precision = match &time.kind {
        LogicalKind::TimestampLtz { .. } | LogicalKind::Timestamp { .. } => 3,
        _ => 3,
    };
    for name in ["window_start", "window_end"] {
        fields.push(ScopeField {
            qualifier: None,
            name: name.into(),
            ty: LogicalType::timestamp(precision).not_null(),
        });
    }
    fields.push(ScopeField {
        qualifier: None,
        name: "window_time".into(),
        ty: window_time.not_null(),
    });
    // A window TVF's input time attribute stays a time attribute in the output.
    Ok(fields)
}

fn bind_table_operand(
    b: &Binder,
    e: &Expr,
    ctx: &Scope,
    _left: &[ScopeField],
) -> BindResult<Vec<ScopeField>> {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let name: ObjectName = match e {
                Expr::Identifier(i) => ObjectName::from(vec![i.clone()]),
                Expr::CompoundIdentifier(parts) => ObjectName::from(parts.clone()),
                _ => unreachable!(),
            };
            let parts = name_parts(&name);
            if parts.len() == 1
                && let Some((_, cols)) = ctx.ctes.iter().rev().find(|(n, _)| *n == parts[0])
            {
                return Ok(cols
                    .iter()
                    .map(|c| ScopeField {
                        qualifier: None,
                        name: c.name.clone(),
                        ty: c.ty.clone(),
                    })
                    .collect());
            }
            let (_, schema) = b.resolve_table(&name)?;
            Ok(schema
                .columns
                .into_iter()
                .map(|c| ScopeField {
                    qualifier: None,
                    name: c.name,
                    ty: c.ty,
                })
                .collect())
        }
        Expr::Subquery(q) => Ok(bind_query_in(b, q, ctx)?
            .into_iter()
            .map(|c| ScopeField {
                qualifier: None,
                name: c.name,
                ty: c.ty,
            })
            .collect()),
        Expr::Nested(inner) => bind_table_operand(b, inner, ctx, _left),
        other => Err(BindError::validation(
            format!("Unsupported table argument: {other}"),
            other.span(),
        )),
    }
}

/// Types an expression against `scope` (used for computed columns and watermarks).
pub fn type_expr(b: &Binder, scope: &Scope, e: &Expr) -> BindResult<LogicalType> {
    type_expr_in(b, scope, e)
}

fn nullable_if(ty: LogicalType, any_nullable: bool) -> LogicalType {
    if any_nullable { ty.nullable() } else { ty }
}

fn integer_rank(ty: &LogicalType) -> Option<u8> {
    match ty.kind {
        LogicalKind::TinyInt => Some(1),
        LogicalKind::SmallInt => Some(2),
        LogicalKind::Int => Some(3),
        LogicalKind::BigInt => Some(4),
        _ => None,
    }
}

fn decimal_of(ty: &LogicalType) -> Option<Decimal> {
    as_decimal(ty).or(match ty.kind {
        LogicalKind::TinyInt => Some(Decimal::new(3, 0)),
        LogicalKind::SmallInt => Some(Decimal::new(5, 0)),
        LogicalKind::Int => Some(Decimal::new(10, 0)),
        LogicalKind::BigInt => Some(Decimal::new(19, 0)),
        _ => None,
    })
}

fn arithmetic(
    op: &BinaryOperator,
    l: &LogicalType,
    r: &LogicalType,
    span: Span,
) -> BindResult<LogicalType> {
    let nullable = l.nullable || r.nullable;
    let l = materialize(l);
    let r = materialize(r);
    // Datetime arithmetic.
    let datetime = |t: &LogicalType| t.is(LogicalTypeFamily::Datetime);
    let interval = |t: &LogicalType| t.is(LogicalTypeFamily::Interval);
    if datetime(&l) && interval(&r) {
        return Ok(nullable_if(l.with_nullable(false), nullable));
    }
    if interval(&l) && datetime(&r) && matches!(op, BinaryOperator::Plus) {
        return Ok(nullable_if(r.with_nullable(false), nullable));
    }
    if interval(&l) && interval(&r) {
        return Ok(nullable_if(l.with_nullable(false), nullable));
    }
    if interval(&l) && r.is(LogicalTypeFamily::Numeric) {
        return Ok(nullable_if(l.with_nullable(false), nullable));
    }
    if l.is(LogicalTypeFamily::Numeric) && interval(&r) && matches!(op, BinaryOperator::Multiply) {
        return Ok(nullable_if(r.with_nullable(false), nullable));
    }
    if matches!(l.kind, LogicalKind::Null) {
        return Ok(r.nullable());
    }
    if matches!(r.kind, LogicalKind::Null) {
        return Ok(l.nullable());
    }
    let numeric = |t: &LogicalType| t.is(LogicalTypeFamily::Numeric);
    if !numeric(&l) || !numeric(&r) {
        // Flink coerces character operands of arithmetic to the numeric side's type.
        let other = if numeric(&l) {
            &l
        } else if numeric(&r) {
            &r
        } else {
            return Err(BindError::validation(
                format!(
                    "Cannot apply '{op}' to arguments of type '<{}> {op} <{}>'. Supported form(s): '<NUMERIC> {op} <NUMERIC>'",
                    l.with_nullable(true).to_string().to_uppercase(),
                    r.with_nullable(true).to_string().to_uppercase()
                ),
                span,
            ));
        };
        if l.is(LogicalTypeFamily::CharacterString) || r.is(LogicalTypeFamily::CharacterString) {
            // Calcite's implicit coercion casts the character operand to the numeric side.
            return Ok(other.clone().nullable());
        }
        return Err(BindError::validation(
            format!("Cannot apply '{op}' to arguments of type '<{l}> {op} <{r}>'"),
            span,
        ));
    }
    let approx = |t: &LogicalType| t.is(LogicalTypeFamily::ApproximateNumeric);
    let ty = if approx(&l) || approx(&r) {
        if matches!(l.kind, LogicalKind::Float) && matches!(r.kind, LogicalKind::Float) {
            LogicalType::float()
        } else {
            LogicalType::double()
        }
    } else if let (Some(a), Some(c)) = (integer_rank(&l), integer_rank(&r)) {
        let rank = a.max(c);
        match rank {
            1 => LogicalType::tinyint(),
            2 => LogicalType::smallint(),
            3 => LogicalType::int(),
            _ => LogicalType::bigint(),
        }
    } else {
        let (Some(a), Some(c)) = (decimal_of(&l), decimal_of(&r)) else {
            return Ok(nullable_if(LogicalType::double(), nullable));
        };
        let d = match op {
            BinaryOperator::Plus | BinaryOperator::Minus => {
                addition_decimal(a.precision, a.scale, c.precision, c.scale)
            }
            BinaryOperator::Multiply => {
                multiplication_decimal(a.precision, a.scale, c.precision, c.scale)
            }
            BinaryOperator::Divide => division_decimal(a.precision, a.scale, c.precision, c.scale),
            _ => modulo_decimal(a.precision, a.scale, c.precision, c.scale),
        };
        d.to_type()
    };
    Ok(ty.with_nullable(nullable))
}

fn number_literal(s: &str) -> LogicalType {
    if s.contains(['e', 'E']) {
        return LogicalType::double().not_null();
    }
    if let Some(dot) = s.find('.') {
        let digits = s.chars().filter(|c| c.is_ascii_digit()).count();
        let scale = s.len() - dot - 1;
        let precision = digits.max(1).max(scale);
        return LogicalType::decimal(precision.min(38) as u8, scale.min(38) as u8).not_null();
    }
    match s.parse::<i64>() {
        Ok(v) if i32::try_from(v).is_ok() => LogicalType::int().not_null(),
        Ok(_) => LogicalType::bigint().not_null(),
        Err(_) => LogicalType::decimal(s.len().min(38) as u8, 0).not_null(),
    }
}

fn fractional_digits(s: &str) -> u8 {
    s.split('.')
        .nth(1)
        .map(|f| f.trim().len().min(9) as u8)
        .unwrap_or(0)
}

fn type_value(v: &Value) -> LogicalType {
    match v {
        Value::Number(n, _) => number_literal(n),
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::EscapedStringLiteral(s)
        | Value::UnicodeStringLiteral(s)
        | Value::NationalStringLiteral(s) => LogicalType::char(s.chars().count() as u32).not_null(),
        Value::HexStringLiteral(s)
        | Value::SingleQuotedByteStringLiteral(s)
        | Value::DoubleQuotedByteStringLiteral(s) => {
            LogicalType::new(LogicalKind::Binary(s.len().div_ceil(2).max(1) as u32)).not_null()
        }
        Value::Boolean(_) => LogicalType::boolean().not_null(),
        Value::Null => LogicalType::null(),
        _ => LogicalType::string(),
    }
}

fn interval_type(field: Option<&DateTimeField>, last: Option<&DateTimeField>) -> LogicalType {
    let year_month = |f: Option<&DateTimeField>| {
        matches!(
            f,
            Some(
                DateTimeField::Year
                    | DateTimeField::Years
                    | DateTimeField::Month
                    | DateTimeField::Months
            )
        )
    };
    if year_month(field) && (last.is_none() || year_month(last)) {
        LogicalType::interval_month().not_null()
    } else {
        LogicalType::interval_second(3).not_null()
    }
}

pub(crate) fn type_expr_in(b: &Binder, scope: &Scope, e: &Expr) -> BindResult<LogicalType> {
    let span = e.span();
    let ty = |x: &Expr| type_expr_in(b, scope, x);
    match e {
        Expr::Identifier(i) => scope.resolve(None, &i.value, i.span),
        Expr::CompoundIdentifier(parts) => {
            // `t.c`, `t.c.field`, or `c.field` (row field access).
            let names: Vec<&str> = parts.iter().map(|p| p.value.as_str()).collect();
            if names.len() >= 2
                && let Ok(t) = scope.resolve(Some(names[0]), names[1], span)
            {
                return access_fields(t, &names[2..], span);
            }
            if names.len() >= 3 && names.len() >= 3 {
                // catalog-qualified columns are not valid; try `c.f1.f2` as nested access.
            }
            let base = scope.resolve(None, names[0], span).map_err(|_| {
                BindError::validation(
                    format!("Column '{}' not found in any table", names.join(".")),
                    span,
                )
            })?;
            access_fields(base, &names[1..], span)
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            let mut t = ty(root)?;
            for a in access_chain {
                t = match a {
                    AccessExpr::Dot(Expr::Identifier(i)) => {
                        access_fields(t, &[i.value.as_str()], span)?
                    }
                    AccessExpr::Dot(other) => {
                        let n = other.to_string();
                        access_fields(t, &[n.as_str()], span)?
                    }
                    AccessExpr::Subscript(Subscript::Index { index }) => {
                        ty(index)?;
                        match &t.kind {
                            LogicalKind::Array(el) => (**el).clone().nullable(),
                            LogicalKind::Map(_, v) => (**v).clone().nullable(),
                            LogicalKind::Row(_) => LogicalType::string(),
                            _ => {
                                return Err(BindError::validation(
                                    format!(
                                        "Cannot apply 'ITEM' to arguments of type 'ITEM(<{t}>, <...>)'"
                                    ),
                                    span,
                                ));
                            }
                        }
                    }
                    AccessExpr::Subscript(Subscript::Slice { .. }) => t.clone(),
                };
            }
            Ok(t)
        }
        Expr::Value(v) => Ok(type_value(&v.value)),
        Expr::TypedString(t) => {
            let text = t.value.value.to_string();
            let text = text.trim_matches('\'');
            Ok(match &t.data_type {
                DataType::Date => LogicalType::date().not_null(),
                DataType::Time(..) => LogicalType::time(0).not_null(),
                DataType::Timestamp(_, tz) => {
                    let p = fractional_digits(text);
                    if matches!(tz, sqlparser::ast::TimezoneInfo::WithTimeZone) {
                        LogicalType::timestamp_ltz(p).not_null()
                    } else {
                        LogicalType::timestamp(p).not_null()
                    }
                }
                other => {
                    match type_of_cast(other).and_then(|f| LogicalType::from_syntax(&f).ok()) {
                        Some(t) => t.not_null(),
                        None => LogicalType::string().not_null(),
                    }
                }
            })
        }
        Expr::Interval(i) => {
            ty(&i.value)?;
            Ok(interval_type(
                i.leading_field.as_ref(),
                i.last_field.as_ref(),
            ))
        }
        Expr::Nested(inner) => ty(inner),
        Expr::IsNull(x)
        | Expr::IsNotNull(x)
        | Expr::IsTrue(x)
        | Expr::IsNotTrue(x)
        | Expr::IsFalse(x)
        | Expr::IsNotFalse(x)
        | Expr::IsUnknown(x)
        | Expr::IsNotUnknown(x) => {
            ty(x)?;
            Ok(LogicalType::boolean().not_null())
        }
        Expr::IsDistinctFrom(a, c) | Expr::IsNotDistinctFrom(a, c) => {
            ty(a)?;
            ty(c)?;
            Ok(LogicalType::boolean().not_null())
        }
        Expr::InList { expr, list, .. } => {
            let mut nullable = ty(expr)?.nullable;
            for x in list {
                nullable |= ty(x)?.nullable;
            }
            Ok(LogicalType::boolean().with_nullable(nullable))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            ty(expr)?;
            bind_query_in(b, subquery, scope)?;
            Ok(LogicalType::boolean())
        }
        Expr::Exists { subquery, .. } => {
            bind_query_in(b, subquery, scope)?;
            Ok(LogicalType::boolean().not_null())
        }
        Expr::Subquery(q) => {
            let out = bind_query_in(b, q, scope)?;
            if out.len() != 1 {
                return Err(BindError::validation(
                    "Scalar sub-query must return exactly one column",
                    span,
                ));
            }
            Ok(out[0].ty.clone().nullable())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let n = ty(expr)?.nullable | ty(low)?.nullable | ty(high)?.nullable;
            Ok(LogicalType::boolean().with_nullable(n))
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => {
            let n = ty(expr)?.nullable | ty(pattern)?.nullable;
            Ok(LogicalType::boolean().with_nullable(n))
        }
        Expr::RLike { expr, pattern, .. } => {
            let n = ty(expr)?.nullable | ty(pattern)?.nullable;
            Ok(LogicalType::boolean().with_nullable(n))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = ty(left)?;
            let r = ty(right)?;
            let nullable = l.nullable || r.nullable;
            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => arithmetic(op, &l, &r, span),
                BinaryOperator::StringConcat => {
                    let len = match (&l.kind, &r.kind) {
                        (LogicalKind::Char(a), LogicalKind::Char(c)) => Some(a.saturating_add(*c)),
                        _ => None,
                    };
                    Ok(match len {
                        Some(n) => LogicalType::char(n),
                        None => LogicalType::string(),
                    }
                    .with_nullable(nullable))
                }
                BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                    Ok(LogicalType::boolean().with_nullable(nullable))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Spaceship => {
                    comparable(&l, &r, op, span)?;
                    Ok(LogicalType::boolean().with_nullable(nullable))
                }
                BinaryOperator::BitwiseAnd
                | BinaryOperator::BitwiseOr
                | BinaryOperator::BitwiseXor => Ok(materialize(&l).with_nullable(nullable)),
                _ => Ok(LogicalType::boolean().with_nullable(nullable)),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let t = ty(expr)?;
            Ok(match op {
                UnaryOperator::Not => LogicalType::boolean().with_nullable(t.nullable),
                _ => materialize(&t),
            })
        }
        Expr::Cast {
            expr,
            data_type,
            kind,
            ..
        } => {
            let source = ty(expr)?;
            let target = type_of_cast(data_type)
                .ok_or_else(|| BindError::validation(format!("Unknown type {data_type}"), span))
                .and_then(|f| {
                    LogicalType::from_syntax(&f)
                        .map_err(|err| BindError::validation(err.to_string(), span))
                })?;
            if !matches!(source.kind, LogicalKind::Null)
                && !cast_allowed_sql(&materialize(&source), &target)
            {
                return Err(BindError::validation(
                    format!(
                        "Cast function cannot convert value of type {} to type {}",
                        materialize(&source)
                            .with_nullable(true)
                            .to_string()
                            .to_uppercase(),
                        target.with_nullable(true).to_string().to_uppercase()
                    ),
                    span,
                ));
            }
            let try_cast = matches!(
                kind,
                sqlparser::ast::CastKind::TryCast | sqlparser::ast::CastKind::SafeCast
            );
            Ok(target
                .with_nullable(source.nullable || try_cast || target.nullable && source.nullable))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                ty(o)?;
            }
            let mut results = vec![];
            for when in conditions {
                ty(&when.condition)?;
                results.push(ty(&when.result)?);
            }
            match else_result {
                Some(x) => results.push(ty(x)?),
                None => results.push(LogicalType::null()),
            }
            let materialized: Vec<_> = results.iter().map(materialize).collect();
            find_common_type(&materialized).ok_or_else(|| {
                BindError::validation(
                    "Illegal mixing of types in CASE or COALESCE statement",
                    span,
                )
            })
        }
        Expr::Extract { expr, .. } => {
            let t = ty(expr)?;
            Ok(LogicalType::bigint().with_nullable(t.nullable))
        }
        Expr::Ceil { expr, field } | Expr::Floor { expr, field } => {
            let t = materialize(&ty(expr)?);
            Ok(match field {
                CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => match t.kind {
                    LogicalKind::Decimal { precision, .. } => {
                        LogicalType::decimal(precision, 0).with_nullable(t.nullable)
                    }
                    _ => t,
                },
                _ => t,
            })
        }
        Expr::Position { expr, r#in } => {
            let n = ty(expr)?.nullable | ty(r#in)?.nullable;
            Ok(LogicalType::int().with_nullable(n))
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let t = ty(expr)?;
            if let Some(x) = substring_from {
                ty(x)?;
            }
            if let Some(x) = substring_for {
                ty(x)?;
            }
            Ok(if t.is(LogicalTypeFamily::BinaryString) {
                LogicalType::bytes()
            } else {
                LogicalType::string()
            }
            .with_nullable(true))
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            let t = ty(expr)?;
            if let Some(w) = trim_what {
                ty(w)?;
            }
            Ok(LogicalType::string().with_nullable(t.nullable))
        }
        Expr::Overlay { expr, .. } => {
            let t = ty(expr)?;
            Ok(LogicalType::string().with_nullable(t.nullable))
        }
        Expr::Array(a) => {
            let tys = a.elem.iter().map(ty).collect::<BindResult<Vec<_>>>()?;
            let el = find_common_type(&tys).unwrap_or_else(LogicalType::null);
            Ok(LogicalType::array(el).not_null())
        }
        Expr::Tuple(items) => {
            let fields = items
                .iter()
                .enumerate()
                .map(|(i, x)| Ok(LogicalRowField::new(format!("EXPR${i}"), ty(x)?)))
                .collect::<BindResult<Vec<_>>>()?;
            Ok(LogicalType::row(fields).not_null())
        }
        Expr::Function(f) => {
            if let Some(args) = map_constructor(e) {
                let tys = args
                    .iter()
                    .filter_map(arg_expr)
                    .map(ty)
                    .collect::<BindResult<Vec<_>>>()?;
                let keys: Vec<_> = tys.iter().step_by(2).cloned().collect();
                let values: Vec<_> = tys.iter().skip(1).step_by(2).cloned().collect();
                let k = find_common_type(&keys).unwrap_or_else(LogicalType::null);
                let v = find_common_type(&values).unwrap_or_else(LogicalType::null);
                return Ok(LogicalType::map(k, v).not_null());
            }
            type_function(b, scope, f, span)
        }
        Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => Ok(LogicalType::null()),
        Expr::AtTimeZone { .. } => {
            Err(BindError::validation("AT TIME ZONE is not Flink SQL", span))
        }
        other => Err(BindError::validation(
            format!("Unsupported expression: {other}"),
            span,
        )),
    }
}

fn comparable(l: &LogicalType, r: &LogicalType, op: &BinaryOperator, span: Span) -> BindResult<()> {
    let (l, r) = (materialize(l), materialize(r));
    if matches!(l.kind, LogicalKind::Null) || matches!(r.kind, LogicalKind::Null) {
        return Ok(());
    }
    if find_common_type(&[l.clone(), r.clone()]).is_some() {
        return Ok(());
    }
    // Flink coerces character strings to the other side's type in comparisons (Calcite's
    // implicit type coercion), so only clearly incompatible families are refused.
    let char_side =
        l.is(LogicalTypeFamily::CharacterString) || r.is(LogicalTypeFamily::CharacterString);
    if char_side {
        return Ok(());
    }
    let numeric_or_interval =
        |t: &LogicalType| t.is(LogicalTypeFamily::Numeric) || t.is(LogicalTypeFamily::Interval);
    if (l.is(LogicalTypeFamily::Datetime) && r.is(LogicalTypeFamily::Datetime))
        || (numeric_or_interval(&l) && numeric_or_interval(&r))
    {
        return Ok(());
    }
    Err(BindError::validation(
        format!(
            "Cannot apply '{op}' to arguments of type '<{}> {op} <{}>'. Supported form(s): '<COMPARABLE_TYPE> {op} <COMPARABLE_TYPE>'",
            l.with_nullable(true).to_string().to_uppercase(),
            r.with_nullable(true).to_string().to_uppercase()
        ),
        span,
    ))
}

fn access_fields(mut t: LogicalType, path: &[&str], span: Span) -> BindResult<LogicalType> {
    for p in path {
        let nullable = t.nullable;
        t = match &t.kind {
            LogicalKind::Row(fields) => match fields.iter().find(|f| f.name == *p) {
                Some(f) => f.ty.clone().with_nullable(f.ty.nullable || nullable),
                None => {
                    return Err(BindError::validation(format!("Unknown field '{p}'"), span));
                }
            },
            _ => {
                return Err(BindError::validation(
                    format!("Column '{p}' not found in any table"),
                    span,
                ));
            }
        };
    }
    Ok(t)
}

fn type_function(b: &Binder, scope: &Scope, f: &Function, span: Span) -> BindResult<LogicalType> {
    let name = name_parts(&f.name);
    let upper = name.join(".").to_uppercase();
    let args = function_args(f);
    let mut types = vec![];
    let mut star = false;
    for arg in &args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => star = true,
            _ => {
                if let Some(e) = arg_expr(arg) {
                    if let Expr::Function(d) = e
                        && name_parts(&d.name)
                            .join(".")
                            .eq_ignore_ascii_case("DESCRIPTOR")
                    {
                        types.push(LogicalType::new(LogicalKind::Descriptor));
                        continue;
                    }
                    if table_argument(e).is_some() {
                        types.push(LogicalType::new(LogicalKind::Null));
                        continue;
                    }
                    types.push(type_expr_in(b, scope, e)?);
                }
            }
        }
    }
    if let Some(filter) = &f.filter {
        type_expr_in(b, scope, filter)?;
    }
    if let Some(res) = functions::builtin(&upper, &types, star, f.over.is_some()) {
        return res.map_err(|m| BindError::validation(m, span));
    }
    if b.user_function(&f.name).is_some() {
        return Err(BindError::unsupported(
            Feature::USER_DEFINED_FUNCTIONS,
            span,
        ));
    }
    Err(BindError::validation(
        format!(
            "No match found for function signature {}({})",
            name.join("."),
            types
                .iter()
                .map(|t| format!("<{}>", functions::sql_family(t)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        span,
    ))
}
