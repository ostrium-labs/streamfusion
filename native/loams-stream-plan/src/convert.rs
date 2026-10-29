//! SQL to the logical plan: the planner's counterpart of Calcite's `SqlToRelConverter` plus the
//! Flink rewrites that give a streaming plan its shape (FC1 Task 10).
//!
//! The binder validates every statement first, so this module may assume names resolve and
//! types check; it types each expression with the binder's rules
//! ([`loams_stream_sql::binder::type_expr`]) and builds [`Rel`] trees. Constructs it does not
//! convert yet fail with a named feature (D2268), never silently.

use std::collections::BTreeMap;

use loams_stream_sql::binder::{
    Binder, CatalogObject, ColumnKind, ObjectIdentifier, Scope, ScopeField, type_expr,
};
use loams_stream_sql::sqlparser::ast::{
    self as sql, AccessExpr, BinaryOperator, CastKind, DateTimeField, Expr, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, ObjectName,
    ObjectNamePart, OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier,
    Subscript, TableFactor, TableWithJoins, UnaryOperator, Value,
};
use loams_stream_sql::types::{LogicalKind, LogicalType};
use loams_stream_sql::{Insert, Statement, table_argument};

use crate::error::{PlanError, PlanResult};
use crate::expr::{Literal, Op, ScalarExpr, materialized};
use crate::logical::{
    AggCall, AggFunc, Field, JoinKind, RankFunc, Rel, SinkPlan, SortKey, TableRef, WindowKind,
};

/// A field visible to expressions, with its position in the current relation.
#[derive(Debug, Clone)]
struct ScopeEntry {
    qualifier: Option<String>,
    name: String,
    ty: LogicalType,
}

/// The fields of the relation an expression is evaluated over.
#[derive(Debug, Clone, Default)]
struct RelScope {
    entries: Vec<ScopeEntry>,
}

impl RelScope {
    fn of(rel: &Rel, qualifier: Option<&str>) -> Self {
        RelScope {
            entries: rel
                .fields()
                .iter()
                .map(|f| ScopeEntry {
                    qualifier: qualifier.map(str::to_owned),
                    name: f.name.clone(),
                    ty: f.ty.clone(),
                })
                .collect(),
        }
    }

    fn binder_scope(&self) -> Scope {
        Scope {
            fields: self
                .entries
                .iter()
                .map(|e| ScopeField {
                    qualifier: e.qualifier.clone(),
                    name: e.name.clone(),
                    ty: e.ty.clone(),
                })
                .collect(),
            ctes: vec![],
        }
    }

    fn find(&self, qualifier: Option<&str>, name: &str) -> Vec<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                e.name == name && qualifier.is_none_or(|q| e.qualifier.as_deref() == Some(q))
            })
            .map(|(i, _)| i)
            .collect()
    }

    fn resolve(&self, qualifier: Option<&str>, name: &str) -> Option<usize> {
        let found = self.find(qualifier, name);
        match found.as_slice() {
            [i] => Some(*i),
            [first, ..] if qualifier.is_some() => Some(*first),
            _ => None,
        }
    }

    fn requalify(mut self, qualifier: Option<&str>) -> Self {
        for e in self.entries.iter_mut() {
            e.qualifier = qualifier.map(str::to_owned);
        }
        self
    }
}

/// A converted `FROM` item: its relation and the names its fields are visible under.
struct From {
    rel: Rel,
    scope: RelScope,
}

/// Converts statements of one session.
pub struct Converter<'a> {
    binder: &'a Binder,
    ctes: Vec<(String, Rel)>,
}

/// The aggregate functions the converter recognises in a select list.
fn agg_func(name: &str, star: bool) -> Option<AggFunc> {
    Some(match name {
        "COUNT" if star => AggFunc::CountStar,
        "COUNT" => AggFunc::Count,
        "SUM" => AggFunc::Sum,
        "SUM0" | "$SUM0" => AggFunc::Sum0,
        "MIN" => AggFunc::Min,
        "MAX" => AggFunc::Max,
        "AVG" => AggFunc::Avg,
        "FIRST_VALUE" => AggFunc::FirstValue,
        "LAST_VALUE" => AggFunc::LastValue,
        "LISTAGG" => AggFunc::ListAgg,
        "COLLECT"
        | "STDDEV_POP"
        | "STDDEV_SAMP"
        | "VAR_POP"
        | "VAR_SAMP"
        | "STDDEV"
        | "VARIANCE"
        | "ARRAY_AGG"
        | "JSON_ARRAYAGG"
        | "JSON_OBJECTAGG"
        | "BIT_AND"
        | "BIT_OR"
        | "BIT_XOR"
        | "APPROX_COUNT_DISTINCT"
        | "SINGLE_VALUE" => AggFunc::Other(name.to_owned()),
        _ => return None,
    })
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .collect()
}

fn function_name(f: &sql::Function) -> String {
    object_name_parts(&f.name).join(".").to_uppercase()
}

fn function_args(f: &sql::Function) -> Vec<&FunctionArg> {
    match &f.args {
        FunctionArguments::List(list) => list.args.iter().collect(),
        _ => vec![],
    }
}

fn is_distinct(f: &sql::Function) -> bool {
    matches!(&f.args, FunctionArguments::List(l)
        if matches!(l.duplicate_treatment, Some(sql::DuplicateTreatment::Distinct)))
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

fn is_star(f: &sql::Function) -> bool {
    function_args(f).iter().any(|a| {
        matches!(
            a,
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_))
        )
    })
}

/// Whether `e` contains an aggregate call outside an `OVER` window.
fn contains_aggregate(e: &Expr) -> bool {
    let mut found = false;
    let _ = sql::visit_expressions(e, |x: &Expr| {
        if let Expr::Function(f) = x
            && f.over.is_none()
            && agg_func(&function_name(f), is_star(f)).is_some()
        {
            found = true;
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

fn contains_over(e: &Expr) -> bool {
    let mut found = false;
    let _ = sql::visit_expressions(e, |x: &Expr| {
        if let Expr::Function(f) = x
            && f.over.is_some()
        {
            found = true;
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

fn contains_subquery(e: &Expr) -> bool {
    let mut found = false;
    let _ = sql::visit_expressions(e, |x: &Expr| {
        if matches!(
            x,
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }
        ) {
            found = true;
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

/// The common type of two numeric operands for a comparison or `CASE` (nullability aside).
fn bool_type(nullable: bool) -> LogicalType {
    LogicalType::boolean().with_nullable(nullable)
}

/// Parses `s` as a SQL expression with the Flink dialect.
pub fn parse_expr(s: &str) -> PlanResult<Expr> {
    let statement = loams_stream_sql::parse_statement(&format!("SELECT {s}"))?;
    let Statement::Query(q) = statement else {
        return Err(PlanError::invalid(format!("not an expression: {s}")));
    };
    let SetExpr::Select(select) = q.query.body.as_ref() else {
        return Err(PlanError::invalid(format!("not an expression: {s}")));
    };
    match select.projection.first() {
        Some(SelectItem::UnnamedExpr(e)) => Ok(e.clone()),
        Some(SelectItem::ExprWithAlias { expr, .. }) => Ok(expr.clone()),
        _ => Err(PlanError::invalid(format!("not an expression: {s}"))),
    }
}

/// Interval text in milliseconds or months.
pub fn interval_value(
    text: &str,
    leading: Option<&DateTimeField>,
    last: Option<&DateTimeField>,
) -> PlanResult<Literal> {
    use DateTimeField as F;
    let bad = || PlanError::invalid(format!("Illegal interval literal '{text}'"));
    let t = text.trim();
    let (negative, t) = match t.strip_prefix('-') {
        Some(rest) => (true, rest.trim()),
        None => (false, t.strip_prefix('+').unwrap_or(t).trim()),
    };
    let int = |s: &str| s.trim().parse::<i64>().map_err(|_| bad());
    let seconds_ms = |s: &str| -> PlanResult<i64> {
        let s = s.trim();
        match s.split_once('.') {
            Some((w, f)) => {
                let mut frac: String = f.chars().take(3).collect();
                while frac.len() < 3 {
                    frac.push('0');
                }
                Ok(int(w)? * 1000 + int(&frac)?)
            }
            None => Ok(int(s)? * 1000),
        }
    };
    let canon = |f: Option<&F>| -> Option<u8> {
        Some(match f? {
            F::Year | F::Years => 0,
            F::Month | F::Months => 1,
            F::Day | F::Days => 2,
            F::Hour | F::Hours => 3,
            F::Minute | F::Minutes => 4,
            F::Second | F::Seconds => 5,
            _ => return None,
        })
    };
    let lead = canon(leading).ok_or_else(bad)?;
    let tail = match last {
        Some(_) => canon(last).ok_or_else(bad)?,
        None => lead,
    };
    let sign = if negative { -1 } else { 1 };
    if lead <= 1 {
        let months = match (lead, tail) {
            (0, 0) => int(t)? * 12,
            (1, 1) => int(t)?,
            (0, 1) => {
                let (y, m) = t.split_once('-').ok_or_else(bad)?;
                int(y)? * 12 + int(m)?
            }
            _ => return Err(bad()),
        };
        return Ok(Literal::Months((sign * months) as i32));
    }
    const MS: [i64; 4] = [86_400_000, 3_600_000, 60_000, 1000];
    // Split "d h:m:s.f" into its parts, from the leading field on.
    let parts: Vec<&str> = t.split([' ', ':']).filter(|p| !p.is_empty()).collect();
    let fields = (tail - lead + 1) as usize;
    if parts.len() != fields {
        return Err(bad());
    }
    let mut total = 0i64;
    for (i, part) in parts.iter().enumerate() {
        let unit = (lead as usize - 2) + i;
        total += if unit == 3 {
            seconds_ms(part)?
        } else {
            int(part)? * MS[unit]
        };
    }
    Ok(Literal::Millis(sign * total))
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `yyyy-MM-dd` as days since the epoch.
pub fn parse_date(s: &str) -> Option<i32> {
    let mut it = s.trim().splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    i32::try_from(days_from_civil(y, m, d)).ok()
}

/// `HH:mm:ss[.fffffffff]` as (milliseconds of the day, nanoseconds within the millisecond).
pub fn parse_time(s: &str) -> Option<(i64, i32)> {
    let s = s.trim();
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    let mut it = hms.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let sec: i64 = it.next().unwrap_or("0").parse().ok()?;
    if frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut nanos_text = frac.to_owned();
    while nanos_text.len() < 9 {
        nanos_text.push('0');
    }
    let nanos: i64 = nanos_text.parse().ok()?;
    let millis = ((h * 60 + m) * 60 + sec) * 1000 + nanos / 1_000_000;
    Some((millis, (nanos % 1_000_000) as i32))
}

/// `yyyy-MM-dd[ HH:mm:ss[.f]]` (or with `T`) as epoch milliseconds and sub-millisecond nanos.
pub fn parse_timestamp(s: &str) -> Option<(i64, i32)> {
    let s = s.trim().trim_end_matches('Z');
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let days = parse_date(date)? as i64;
    let (ms, nanos) = match time {
        Some(t) => parse_time(t)?,
        None => (0, 0),
    };
    Some((days * 86_400_000 + ms, nanos))
}

fn decimal_literal(text: &str) -> Option<(i128, u8)> {
    let (whole, frac) = text.split_once('.').unwrap_or((text, ""));
    let digits = format!("{whole}{frac}");
    Some((digits.parse().ok()?, frac.len() as u8))
}

impl<'a> Converter<'a> {
    pub fn new(binder: &'a Binder) -> Self {
        Converter {
            binder,
            ctes: vec![],
        }
    }

    fn type_of(&self, scope: &RelScope, e: &Expr) -> PlanResult<LogicalType> {
        Ok(type_expr(self.binder, &scope.binder_scope(), e)?)
    }

    // ---------------------------------------------------------------------------------------
    // Statements
    // ---------------------------------------------------------------------------------------

    /// Converts an `INSERT` into its sink and input plan.
    pub fn insert(&mut self, insert: &Insert) -> PlanResult<SinkPlan> {
        let id = self.binder.qualify(&insert.table)?;
        let Some(CatalogObject::Table {
            schema, options, ..
        }) = self.binder.lookup(&id).cloned()
        else {
            return Err(PlanError::invalid(format!(
                "Table {id} is not a table that can be written"
            )));
        };
        if !insert.partition.is_empty() {
            return Err(PlanError::not_yet("INSERT ... PARTITION"));
        }
        if schema.columns.iter().any(|c| {
            matches!(
                c.kind,
                ColumnKind::Metadata {
                    is_virtual: false,
                    ..
                }
            )
        }) {
            return Err(PlanError::not_yet("writable metadata columns"));
        }
        let table = self.table_ref(&id, &schema, &options)?;
        let query = self.query(&insert.source)?;
        let targets: Vec<usize> = if insert.columns.is_empty() {
            (0..table.physical.len()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|c| {
                    let name = object_name_parts(c).last().cloned().unwrap_or_default();
                    table
                        .physical
                        .iter()
                        .position(|f| f.name == name)
                        .ok_or_else(|| {
                            PlanError::invalid(format!("Unknown target column '{name}'"))
                        })
                })
                .collect::<PlanResult<_>>()?
        };
        if targets.len() != query.fields().len() {
            return Err(PlanError::invalid(format!(
                "Column types of query result and sink for '{id}' do not match.\nCause: Different number of columns."
            )));
        }
        let mut exprs = vec![];
        for (i, f) in table.physical.iter().enumerate() {
            match targets.iter().position(|t| *t == i) {
                Some(source) => {
                    let from = &query.fields()[source].ty;
                    let input = ScalarExpr::input(source, from.clone());
                    exprs.push(cast_to(input, &f.ty));
                }
                None => {
                    if !f.ty.nullable {
                        return Err(PlanError::invalid(format!(
                            "Column '{}' has no default value and does not allow NULLs.",
                            f.name
                        )));
                    }
                    exprs.push(ScalarExpr::literal(Literal::Null, f.ty.clone()));
                }
            }
        }
        let names = table.physical.iter().map(|f| f.name.clone()).collect();
        let input = query.project(exprs, names);
        Ok(SinkPlan { table, input })
    }

    fn table_ref(
        &self,
        id: &ObjectIdentifier,
        schema: &loams_stream_sql::binder::ResolvedSchema,
        options: &BTreeMap<String, String>,
    ) -> PlanResult<TableRef> {
        let physical: Vec<Field> = schema
            .columns
            .iter()
            .filter(|c| matches!(c.kind, ColumnKind::Physical))
            .map(|c| Field::new(&c.name, materialized(&c.ty)))
            .collect();
        let primary_key = schema.primary_key.as_ref().map(|keys| {
            keys.iter()
                .filter_map(|k| physical.iter().position(|f| f.name == *k))
                .collect()
        });
        Ok(TableRef {
            id: id.clone(),
            options: options.clone(),
            physical,
            primary_key,
        })
    }

    /// Converts a query.
    pub fn query(&mut self, q: &Query) -> PlanResult<Rel> {
        let saved = self.ctes.len();
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                let mut rel = self.query(&cte.query)?;
                if !cte.alias.columns.is_empty() {
                    let names: Vec<String> = rel
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(i, f)| {
                            cte.alias
                                .columns
                                .get(i)
                                .map(|c| c.name.value.clone())
                                .unwrap_or_else(|| f.name.clone())
                        })
                        .collect();
                    let refs = rel.field_refs();
                    rel = rel.project(refs, names);
                }
                self.ctes.push((cte.alias.name.value.clone(), rel));
            }
        }
        let mut rel = self.set_expr(&q.body)?;
        let result = (|| {
            if let Some(order_by) = &q.order_by {
                let OrderByKind::Expressions(items) = &order_by.kind else {
                    return Err(PlanError::not_yet("ORDER BY ALL"));
                };
                let scope = RelScope::of(&rel, None);
                let mut keys = vec![];
                for item in items {
                    // ORDER BY sees the select list; a qualified name (`o.oid`) refers to the
                    // output column it was selected as.
                    let e = match (&item.expr, self.expr(&scope, &item.expr)) {
                        (_, Ok(e)) => e,
                        (Expr::CompoundIdentifier(parts), Err(err)) => {
                            let last = Expr::Identifier(
                                parts.last().cloned().unwrap_or_else(|| sql::Ident::new("")),
                            );
                            self.expr(&scope, &last).map_err(|_| err)?
                        }
                        (_, Err(err)) => return Err(err),
                    };
                    let Some(index) = e.as_input() else {
                        return Err(PlanError::not_yet("ORDER BY of an expression"));
                    };
                    let ascending = item.options.asc.unwrap_or(true);
                    keys.push(SortKey {
                        index,
                        ascending,
                        // Flink's default: NULLS FIRST for ascending, NULLS LAST for descending.
                        nulls_first: item.options.nulls_first.unwrap_or(ascending),
                    });
                }
                rel = Rel::Sort {
                    input: Box::new(rel),
                    keys,
                    offset: 0,
                    fetch: None,
                };
            }
            if let Some(limit) = &q.limit_clause {
                let (fetch, offset) = match limit {
                    sql::LimitClause::LimitOffset { limit, offset, .. } => (
                        limit.as_ref().map(const_i64).transpose()?,
                        offset
                            .as_ref()
                            .map(|o| const_i64(&o.value))
                            .transpose()?
                            .unwrap_or(0),
                    ),
                    sql::LimitClause::OffsetCommaLimit { offset, limit } => {
                        (Some(const_i64(limit)?), const_i64(offset)?)
                    }
                };
                rel = match rel {
                    Rel::Sort { input, keys, .. } => Rel::Sort {
                        input,
                        keys,
                        offset,
                        fetch,
                    },
                    other => Rel::Sort {
                        input: Box::new(other),
                        keys: vec![],
                        offset,
                        fetch,
                    },
                };
            }
            if let Some(fetch) = &q.fetch {
                let n = fetch.quantity.as_ref().map(const_i64).transpose()?;
                rel = Rel::Sort {
                    input: Box::new(rel),
                    keys: vec![],
                    offset: 0,
                    fetch: n,
                };
            }
            Ok(rel)
        })();
        self.ctes.truncate(saved);
        result
    }

    fn set_expr(&mut self, body: &SetExpr) -> PlanResult<Rel> {
        match body {
            SetExpr::Select(select) => self.select(select),
            SetExpr::Query(q) => self.query(q),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let l = self.set_expr(left)?;
                let r = self.set_expr(right)?;
                match (op, set_quantifier) {
                    (SetOperator::Union, SetQuantifier::All) => Ok(union(l, r)),
                    (SetOperator::Union, _) => {
                        let u = union(l, r);
                        Ok(distinct(u))
                    }
                    (SetOperator::Except, _) => Err(PlanError::not_yet("EXCEPT")),
                    (SetOperator::Intersect, _) => Err(PlanError::not_yet("INTERSECT")),
                    _ => Err(PlanError::invalid(format!(
                        "Unsupported set operation {op}"
                    ))),
                }
            }
            SetExpr::Values(values) => self.values(values),
            SetExpr::Table(t) => {
                let name = t.table_name.clone().unwrap_or_default();
                let name = ObjectName::from(vec![sql::Ident::new(name)]);
                Ok(self.table(&name)?.rel)
            }
            other => Err(PlanError::not_yet(format!("query form {other}"))),
        }
    }

    fn values(&mut self, values: &sql::Values) -> PlanResult<Rel> {
        let empty = RelScope::default();
        let mut rows = vec![];
        for row in &values.rows {
            let mut out = vec![];
            for e in row.iter() {
                let x = self.expr(&empty, e)?;
                if contains_non_literal(&x) {
                    return Err(PlanError::not_yet("VALUES of non-constant expressions"));
                }
                out.push(x);
            }
            rows.push(out);
        }
        let Some(first) = rows.first() else {
            return Err(PlanError::invalid("VALUES without rows"));
        };
        let mut fields = vec![];
        for i in 0..first.len() {
            let types: Vec<LogicalType> = rows.iter().map(|r| r[i].ty().clone()).collect();
            let ty = loams_stream_sql::types::merging::find_common_type(&types)
                .unwrap_or_else(|| types[0].clone());
            fields.push(Field::new(format!("EXPR${i}"), ty));
        }
        let rows = rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .zip(&fields)
                    .map(|(e, f)| cast_to(e, &f.ty))
                    .collect()
            })
            .collect();
        Ok(Rel::Values { rows, fields })
    }

    // ---------------------------------------------------------------------------------------
    // FROM
    // ---------------------------------------------------------------------------------------

    fn table(&mut self, name: &ObjectName) -> PlanResult<From> {
        let parts = object_name_parts(name);
        if parts.len() == 1
            && let Some((_, rel)) = self.ctes.iter().rev().find(|(n, _)| *n == parts[0])
        {
            let rel = rel.clone();
            let scope = RelScope::of(&rel, Some(&parts[0]));
            return Ok(From { rel, scope });
        }
        let id = self.binder.qualify(name)?;
        let Some(object) = self.binder.lookup(&id).cloned() else {
            return Err(PlanError::invalid(format!(
                "Object '{}' not found",
                parts.join(".")
            )));
        };
        let qualifier = parts.last().cloned();
        let rel = match object {
            CatalogObject::View { query, .. } => {
                let statement = loams_stream_sql::parse_statement(&query)?;
                let Statement::Query(q) = statement else {
                    return Err(PlanError::invalid(format!("view {id} is not a query")));
                };
                let mut inner = Converter::new(self.binder);
                inner.query(&q.query)?
            }
            CatalogObject::Table {
                schema, options, ..
            } => self.scan(&id, &schema, &options)?,
        };
        let scope = RelScope::of(&rel, qualifier.as_deref());
        Ok(From { rel, scope })
    }

    /// A table scan: the physical columns, computed columns, then the watermark.
    fn scan(
        &mut self,
        id: &ObjectIdentifier,
        schema: &loams_stream_sql::binder::ResolvedSchema,
        options: &BTreeMap<String, String>,
    ) -> PlanResult<Rel> {
        if schema
            .columns
            .iter()
            .any(|c| matches!(c.kind, ColumnKind::Metadata { .. }))
        {
            return Err(PlanError::not_yet("metadata columns"));
        }
        let table = self.table_ref(id, schema, options)?;
        let fields = table.physical.clone();
        let mut rel = Rel::Scan { table, fields };
        let computed = schema
            .columns
            .iter()
            .any(|c| matches!(c.kind, ColumnKind::Computed(_)));
        if computed {
            let scope = RelScope::of(&rel, None);
            let mut exprs = vec![];
            let mut names = vec![];
            let mut physical = 0;
            for c in &schema.columns {
                names.push(c.name.clone());
                match &c.kind {
                    ColumnKind::Computed(text) => {
                        let e = parse_expr(text)?;
                        let x = self.expr(&scope, &e)?;
                        exprs.push(retype(x, &c.ty));
                    }
                    _ => {
                        exprs.push(ScalarExpr::input(
                            physical,
                            rel.fields()[physical].ty.clone(),
                        ));
                        physical += 1;
                    }
                }
            }
            rel = rel.project(exprs, names);
        }
        if let Some((column, text)) = &schema.watermark {
            let scope = RelScope::of(&rel, None);
            let Some(rowtime) = scope.resolve(None, column) else {
                return Err(PlanError::not_yet(format!(
                    "a watermark on the nested field {column}"
                )));
            };
            let e = parse_expr(text)?;
            let expr = self.expr(&scope, &e)?;
            // The rowtime column becomes a time attribute from here on.
            let ty = schema
                .column(column)
                .map(|c| c.ty.clone())
                .unwrap_or_else(|| rel.fields()[rowtime].ty.clone());
            let mut exprs = rel.field_refs();
            let names: Vec<String> = rel.fields().iter().map(|f| f.name.clone()).collect();
            exprs[rowtime] = retype(exprs[rowtime].clone(), &ty);
            let input = rel.project(exprs, names);
            rel = Rel::Watermark {
                input: Box::new(input),
                rowtime,
                expr,
            };
        }
        Ok(rel)
    }

    fn table_with_joins(&mut self, twj: &TableWithJoins) -> PlanResult<From> {
        let mut from = self.table_factor(&twj.relation)?;
        for join in &twj.joins {
            from = self.join(from, join)?;
        }
        Ok(from)
    }

    fn join(&mut self, left: From, join: &sql::Join) -> PlanResult<From> {
        let (kind, constraint) = match &join.join_operator {
            JoinOperator::Join(c) | JoinOperator::Inner(c) => (JoinKind::Inner, c),
            JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
            JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinKind::Right, c),
            JoinOperator::FullOuter(c) => (JoinKind::Full, c),
            JoinOperator::CrossJoin(c) => (JoinKind::Inner, c),
            JoinOperator::Semi(c) | JoinOperator::LeftSemi(c) => (JoinKind::Semi, c),
            JoinOperator::Anti(c) | JoinOperator::LeftAnti(c) => (JoinKind::Anti, c),
            other => return Err(PlanError::not_yet(format!("join {other:?}"))),
        };
        if let TableFactor::Table {
            version: Some(_), ..
        } = &join.relation
        {
            return Err(PlanError::unsupported(
                "temporal joins (FOR SYSTEM_TIME AS OF)",
                "FC2",
            ));
        }
        if matches!(join.relation, TableFactor::UNNEST { .. })
            || matches!(join.relation, TableFactor::Function { .. })
            || matches!(join.relation, TableFactor::TableFunction { .. })
                && !is_window_tvf(&join.relation)
        {
            return Err(PlanError::unsupported(
                "correlated table functions (UNNEST, LATERAL)",
                "FC2",
            ));
        }
        let right = self.table_factor(&join.relation)?;
        let nullable_left = matches!(kind, JoinKind::Right | JoinKind::Full);
        let nullable_right = matches!(kind, JoinKind::Left | JoinKind::Full);
        let mut scope = RelScope::default();
        for e in &left.scope.entries {
            let mut e = e.clone();
            if nullable_left {
                e.ty = e.ty.clone().nullable();
            }
            scope.entries.push(e);
        }
        for e in &right.scope.entries {
            let mut e = e.clone();
            if nullable_right {
                e.ty = e.ty.clone().nullable();
            }
            scope.entries.push(e);
        }
        // The join's condition sees both inputs with their declared nullability.
        let mut cond_scope = left.scope.clone();
        cond_scope
            .entries
            .extend(right.scope.entries.iter().cloned());
        let left_len = left.rel.fields().len();
        let condition =
            match constraint {
                JoinConstraint::On(e) => {
                    if contains_subquery(e) {
                        return Err(PlanError::not_yet("subqueries in a join condition"));
                    }
                    self.expr(&cond_scope, e)?
                }
                JoinConstraint::Using(cols) => {
                    let mut terms = vec![];
                    for c in cols {
                        let name = object_name_parts(c).last().cloned().unwrap_or_default();
                        let l = left.scope.resolve(None, &name).ok_or_else(|| {
                            PlanError::invalid(format!("Column '{name}' not found"))
                        })?;
                        let r = right.scope.resolve(None, &name).ok_or_else(|| {
                            PlanError::invalid(format!("Column '{name}' not found"))
                        })?;
                        let lt = cond_scope.entries[l].ty.clone();
                        let rt = cond_scope.entries[left_len + r].ty.clone();
                        terms.push(ScalarExpr::call(
                            Op::Eq,
                            vec![
                                ScalarExpr::input(l, lt.clone()),
                                ScalarExpr::input(left_len + r, rt.clone()),
                            ],
                            bool_type(lt.nullable || rt.nullable),
                        ));
                    }
                    ScalarExpr::and_all(terms)
                }
                JoinConstraint::Natural => return Err(PlanError::not_yet("NATURAL JOIN")),
                JoinConstraint::None => ScalarExpr::bool(true),
            };
        let semi = matches!(kind, JoinKind::Semi | JoinKind::Anti);
        let fields: Vec<Field> = if semi {
            left.rel.fields().to_vec()
        } else {
            scope
                .entries
                .iter()
                .map(|e| Field::new(&e.name, e.ty.clone()))
                .collect()
        };
        let rel = Rel::Join {
            left: Box::new(left.rel),
            right: Box::new(right.rel),
            kind,
            condition,
            fields,
        };
        let scope = if semi { left.scope } else { scope };
        Ok(From { rel, scope })
    }

    fn table_factor(&mut self, factor: &TableFactor) -> PlanResult<From> {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args,
                version,
                ..
            } => {
                if args.is_some() {
                    return Err(PlanError::unsupported(
                        "process table functions (FLIP-440)",
                        "FC4",
                    ));
                }
                if version.is_some() {
                    return Err(PlanError::unsupported(
                        "temporal joins (FOR SYSTEM_TIME AS OF)",
                        "FC2",
                    ));
                }
                let from = self.table(name)?;
                Ok(alias_from(from, alias.as_ref()))
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
                ..
            } => {
                if *lateral {
                    return Err(PlanError::unsupported("LATERAL subqueries", "FC2"));
                }
                let rel = self.query(subquery)?;
                let scope = RelScope::of(&rel, None);
                Ok(alias_from(From { rel, scope }, alias.as_ref()))
            }
            TableFactor::TableFunction { expr, alias } => {
                let from = self.window_tvf(expr)?;
                Ok(alias_from(from, alias.as_ref()))
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let from = self.table_with_joins(table_with_joins)?;
                Ok(match alias {
                    Some(a) => alias_from(from, Some(a)),
                    None => from,
                })
            }
            TableFactor::UNNEST { .. } | TableFactor::Function { .. } => Err(
                PlanError::unsupported("correlated table functions (UNNEST, LATERAL)", "FC2"),
            ),
            TableFactor::MatchRecognize { .. } => {
                Err(PlanError::unsupported("MATCH_RECOGNIZE", "FC2"))
            }
            other => Err(PlanError::not_yet(format!("table reference {other}"))),
        }
    }

    /// `TABLE(TUMBLE|HOP|CUMULATE|SESSION(TABLE t, DESCRIPTOR(ts), ...))`.
    fn window_tvf(&mut self, expr: &Expr) -> PlanResult<From> {
        let Expr::Function(f) = expr else {
            return Err(PlanError::unsupported("LATERAL table functions", "FC2"));
        };
        let name = function_name(f);
        if !matches!(name.as_str(), "TUMBLE" | "HOP" | "CUMULATE" | "SESSION") {
            return Err(PlanError::unsupported("LATERAL table functions", "FC2"));
        }
        let mut input: Option<From> = None;
        let mut descriptor: Option<String> = None;
        let mut intervals: Vec<(Option<String>, i64)> = vec![];
        let mut partitioned = false;
        for arg in function_args(f) {
            let Some(e) = arg_expr(arg) else { continue };
            let named = arg_name(arg);
            if let Some(t) = table_argument(e) {
                partitioned |= !t.partition_by.is_empty();
                input = Some(self.table_operand(t.table)?);
                continue;
            }
            if named.as_deref() == Some("DATA") {
                input = Some(self.table_operand(e)?);
                continue;
            }
            if let Expr::Function(d) = e
                && function_name(d) == "DESCRIPTOR"
            {
                if let Some(first) = function_args(d).first().and_then(|a| arg_expr(a)) {
                    descriptor = match first {
                        Expr::Identifier(i) => Some(i.value.clone()),
                        Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.clone()),
                        _ => None,
                    };
                }
                continue;
            }
            let value = self.expr(&RelScope::default(), e)?;
            let ScalarExpr::Literal {
                value: Literal::Millis(ms),
                ..
            } = value
            else {
                return Err(PlanError::not_yet(format!(
                    "{name} with a non-constant or year-month interval"
                )));
            };
            intervals.push((named, ms));
        }
        let (Some(from), Some(column)) = (input, descriptor) else {
            return Err(PlanError::invalid(format!("Invalid arguments of {name}")));
        };
        let Some(time) = from.scope.resolve(None, &column) else {
            return Err(PlanError::invalid(format!(
                "Column '{column}' not found in any table"
            )));
        };
        let get = |key: &str, pos: usize| -> Option<i64> {
            intervals
                .iter()
                .find(|(n, _)| n.as_deref() == Some(key))
                .or_else(|| intervals.iter().filter(|(n, _)| n.is_none()).nth(pos))
                .map(|(_, v)| *v)
        };
        let window = match name.as_str() {
            "TUMBLE" => WindowKind::Tumble {
                size_ms: get("SIZE", 0).unwrap_or(0),
                offset_ms: get("OFFSET", 1).unwrap_or(0),
            },
            "HOP" => WindowKind::Hop {
                slide_ms: get("SLIDE", 0).unwrap_or(0),
                size_ms: get("SIZE", 1).unwrap_or(0),
                offset_ms: get("OFFSET", 2).unwrap_or(0),
            },
            "CUMULATE" => WindowKind::Cumulate {
                step_ms: get("STEP", 0).unwrap_or(0),
                max_size_ms: get("SIZE", 1).unwrap_or(0),
                offset_ms: get("OFFSET", 2).unwrap_or(0),
            },
            _ => {
                if partitioned {
                    return Err(PlanError::not_yet("SESSION windows with PARTITION BY"));
                }
                WindowKind::Session {
                    gap_ms: get("GAP", 0).unwrap_or(0),
                }
            }
        };
        let time_ty = from.rel.fields()[time].ty.clone();
        let mut fields = from.rel.fields().to_vec();
        fields.push(Field::new(
            "window_start",
            LogicalType::timestamp(3).not_null(),
        ));
        fields.push(Field::new(
            "window_end",
            LogicalType::timestamp(3).not_null(),
        ));
        fields.push(Field::new("window_time", time_ty.not_null()));
        let rel = Rel::WindowTvf {
            input: Box::new(from.rel),
            time,
            window,
            fields,
        };
        let scope = RelScope::of(&rel, None);
        Ok(From { rel, scope })
    }

    fn table_operand(&mut self, e: &Expr) -> PlanResult<From> {
        match e {
            Expr::Identifier(i) => self.table(&ObjectName::from(vec![i.clone()])),
            Expr::CompoundIdentifier(parts) => self.table(&ObjectName::from(parts.clone())),
            Expr::Subquery(q) => {
                let rel = self.query(q)?;
                let scope = RelScope::of(&rel, None);
                Ok(From { rel, scope })
            }
            Expr::Nested(inner) => self.table_operand(inner),
            other => Err(PlanError::invalid(format!(
                "Unsupported table argument: {other}"
            ))),
        }
    }

    // ---------------------------------------------------------------------------------------
    // SELECT
    // ---------------------------------------------------------------------------------------

    fn select(&mut self, select: &Select) -> PlanResult<Rel> {
        let mut from = match select.from.as_slice() {
            [] => {
                let rel = Rel::Values {
                    rows: vec![vec![]],
                    fields: vec![],
                };
                From {
                    rel,
                    scope: RelScope::default(),
                }
            }
            [first, rest @ ..] => {
                let mut from = self.table_with_joins(first)?;
                for twj in rest {
                    let right = self.table_with_joins(twj)?;
                    let mut scope = from.scope.clone();
                    scope.entries.extend(right.scope.entries.iter().cloned());
                    let fields = scope
                        .entries
                        .iter()
                        .map(|e| Field::new(&e.name, e.ty.clone()))
                        .collect();
                    from = From {
                        rel: Rel::Join {
                            left: Box::new(from.rel),
                            right: Box::new(right.rel),
                            kind: JoinKind::Inner,
                            condition: ScalarExpr::bool(true),
                            fields,
                        },
                        scope,
                    };
                }
                from
            }
        };
        if let Some(w) = &select.selection {
            if contains_subquery(w) {
                return Err(PlanError::not_yet("subqueries (IN, EXISTS, scalar)"));
            }
            let condition = self.expr(&from.scope, w)?;
            from.rel = Rel::Filter {
                input: Box::new(from.rel),
                condition,
            };
        }
        let group_exprs: Vec<&Expr> = match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) => {
                if !modifiers.is_empty() {
                    return Err(PlanError::not_yet("GROUP BY modifiers (ROLLUP, CUBE)"));
                }
                for e in exprs {
                    if matches!(e, Expr::GroupingSets(_) | Expr::Cube(_) | Expr::Rollup(_)) {
                        return Err(PlanError::not_yet("GROUPING SETS, CUBE and ROLLUP"));
                    }
                }
                exprs.iter().collect()
            }
            GroupByExpr::All(_) => return Err(PlanError::not_yet("GROUP BY ALL")),
        };
        let items = self.expand_items(select, &from.scope)?;
        let aggregate = !group_exprs.is_empty()
            || items.iter().any(|(e, _)| contains_aggregate(e))
            || select.having.as_ref().is_some_and(contains_aggregate);
        let (rel, exprs, names) = if aggregate {
            self.aggregate(from, &group_exprs, &items, select.having.as_ref())?
        } else {
            if select.having.is_some() {
                return Err(PlanError::invalid("HAVING without GROUP BY or aggregates"));
            }
            self.plain_projection(from, &items)?
        };
        let names = unique_names(names);
        let mut rel = rel.project(exprs, names);
        if matches!(select.distinct, Some(sql::Distinct::Distinct)) {
            rel = distinct(rel);
        } else if matches!(select.distinct, Some(sql::Distinct::On(_))) {
            return Err(PlanError::invalid("DISTINCT ON is not Flink SQL"));
        }
        Ok(rel)
    }

    /// The select items with wildcards expanded, as (expression, name).
    fn expand_items(&self, select: &Select, scope: &RelScope) -> PlanResult<Vec<(Expr, String)>> {
        let mut out = vec![];
        for (i, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(e) => out.push((e.clone(), default_name(e, i))),
                SelectItem::ExprWithAlias { expr, alias } => {
                    out.push((expr.clone(), alias.value.clone()))
                }
                SelectItem::Wildcard(_) => {
                    for e in &scope.entries {
                        out.push((field_expr(e), e.name.clone()));
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    let q = match kind {
                        sql::SelectItemQualifiedWildcardKind::ObjectName(n) => {
                            object_name_parts(n).last().cloned().unwrap_or_default()
                        }
                        sql::SelectItemQualifiedWildcardKind::Expr(e) => e.to_string(),
                    };
                    for e in scope
                        .entries
                        .iter()
                        .filter(|e| e.qualifier.as_deref() == Some(q.as_str()))
                    {
                        out.push((field_expr(e), e.name.clone()));
                    }
                }
                SelectItem::ExprWithAliases { .. } => {
                    return Err(PlanError::not_yet("multiple aliases for one select item"));
                }
            }
        }
        Ok(out)
    }

    fn plain_projection(
        &mut self,
        from: From,
        items: &[(Expr, String)],
    ) -> PlanResult<(Rel, Vec<ScalarExpr>, Vec<String>)> {
        let mut rel = from.rel;
        let scope = from.scope;
        // A ranking function over a window becomes a Top-N; its range comes from an outer filter.
        let over: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, (e, _))| contains_over(e))
            .map(|(i, _)| i)
            .collect();
        let mut rank_field = None;
        if !over.is_empty() {
            if over.len() > 1 {
                return Err(PlanError::unsupported(
                    "OVER aggregates (OverAggregate)",
                    "FC2",
                ));
            }
            let (e, _) = &items[over[0]];
            let Expr::Function(f) = e else {
                return Err(PlanError::unsupported(
                    "OVER aggregates (OverAggregate)",
                    "FC2",
                ));
            };
            let func = match function_name(f).as_str() {
                "ROW_NUMBER" => RankFunc::RowNumber,
                "RANK" => RankFunc::Rank,
                "DENSE_RANK" => RankFunc::DenseRank,
                _ => {
                    return Err(PlanError::unsupported(
                        "OVER aggregates (OverAggregate)",
                        "FC2",
                    ));
                }
            };
            let Some(sql::WindowType::WindowSpec(spec)) = &f.over else {
                return Err(PlanError::not_yet("named windows"));
            };
            let mut partition = vec![];
            for p in &spec.partition_by {
                let x = self.expr(&scope, p)?;
                partition.push(
                    x.as_input()
                        .ok_or_else(|| PlanError::not_yet("PARTITION BY of an expression"))?,
                );
            }
            let mut order = vec![];
            for o in &spec.order_by {
                let x = self.expr(&scope, &o.expr)?;
                let index = x
                    .as_input()
                    .ok_or_else(|| PlanError::not_yet("ORDER BY of an expression in OVER"))?;
                let ascending = o.options.asc.unwrap_or(true);
                order.push(SortKey {
                    index,
                    ascending,
                    nulls_first: o.options.nulls_first.unwrap_or(ascending),
                });
            }
            let mut fields = rel.fields().to_vec();
            fields.push(Field::new("$rank", LogicalType::bigint().not_null()));
            rank_field = Some(fields.len() - 1);
            rel = Rel::Rank {
                input: Box::new(rel),
                func,
                partition,
                order,
                rank_start: 1,
                rank_end: i64::MAX,
                output_rank: true,
                fields,
            };
        }
        let mut exprs = vec![];
        let mut names = vec![];
        for (i, (e, name)) in items.iter().enumerate() {
            if Some(i) == over.first().copied() {
                let index = rank_field.expect("rank field");
                exprs.push(ScalarExpr::input(index, LogicalType::bigint().not_null()));
            } else {
                if contains_subquery(e) {
                    return Err(PlanError::not_yet("subqueries (IN, EXISTS, scalar)"));
                }
                exprs.push(self.expr(&scope, e)?);
            }
            names.push(name.clone());
        }
        Ok((rel, exprs, names))
    }

    /// `GROUP BY`: a projection of the keys and the aggregate arguments, the aggregate, then the
    /// select list over the aggregate's output.
    fn aggregate(
        &mut self,
        from: From,
        group_exprs: &[&Expr],
        items: &[(Expr, String)],
        having: Option<&Expr>,
    ) -> PlanResult<(Rel, Vec<ScalarExpr>, Vec<String>)> {
        let scope = from.scope;
        for e in group_exprs {
            if let Expr::Function(f) = e
                && matches!(function_name(f).as_str(), "TUMBLE" | "HOP" | "SESSION")
            {
                // Nexmark q11 needs it, so it is in FC1's bar even though the profile plans it for
                // FC2.
                return Err(PlanError::not_yet(
                    "group window aggregates (GROUP BY TUMBLE, HOP, SESSION; GroupWindowAggregate)",
                ));
            }
        }
        let mut pre: Vec<ScalarExpr> = vec![];
        let mut pre_names: Vec<String> = vec![];
        let mut keys: Vec<ScalarExpr> = vec![];
        for e in group_exprs {
            let x = self.expr(&scope, e)?;
            if !keys.contains(&x) {
                pre_names.push(match &x {
                    ScalarExpr::Input { index, .. } => scope.entries[*index].name.clone(),
                    _ => format!("$f{}", pre.len()),
                });
                keys.push(x.clone());
                pre.push(x);
            }
        }
        let mut calls: Vec<(AggCall, Expr)> = vec![];
        let mut collect = |this: &Self, e: &Expr| -> PlanResult<()> {
            let mut found: Vec<sql::Function> = vec![];
            let _ = sql::visit_expressions(e, |x: &Expr| {
                if let Expr::Function(f) = x
                    && f.over.is_none()
                    && agg_func(&function_name(f), is_star(f)).is_some()
                {
                    found.push(f.clone());
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            for f in found {
                let key = Expr::Function(f.clone());
                if calls.iter().any(|(_, k)| *k == key) {
                    continue;
                }
                let func = agg_func(&function_name(&f), is_star(&f)).expect("aggregate");
                let mut args = vec![];
                for a in function_args(&f) {
                    if let Some(x) = arg_expr(a) {
                        if contains_aggregate(x) {
                            return Err(PlanError::invalid("Aggregate calls cannot be nested"));
                        }
                        let x = this.expr(&scope, x)?;
                        args.push(push_unique(&mut pre, &mut pre_names, x));
                    }
                }
                let filter = match &f.filter {
                    Some(fe) => {
                        let x = this.expr(&scope, fe)?;
                        Some(push_unique(&mut pre, &mut pre_names, x))
                    }
                    None => None,
                };
                if !f.within_group.is_empty() {
                    return Err(PlanError::not_yet("WITHIN GROUP"));
                }
                let ty = this.type_of(&scope, &key)?;
                calls.push((
                    AggCall {
                        name: format!("{}", key),
                        func,
                        args,
                        distinct: is_distinct(&f),
                        filter,
                        ty,
                    },
                    key,
                ));
            }
            Ok(())
        };
        for (e, _) in items {
            collect(self, e)?;
        }
        if let Some(h) = having {
            collect(self, h)?;
        }
        let pre_rel = from.rel.project(pre.clone(), pre_names.clone());
        let group: Vec<usize> = (0..keys.len()).collect();
        let mut fields: Vec<Field> = group
            .iter()
            .map(|&i| Field::new(&pre_names[i], pre[i].ty().clone()))
            .collect();
        let g = fields.len();
        for (i, (c, _)) in calls.iter().enumerate() {
            fields.push(Field::new(format!("EXPR${i}"), c.ty.clone()));
        }
        let agg_calls: Vec<AggCall> = calls.iter().map(|(c, _)| c.clone()).collect();
        let call_keys: Vec<Expr> = calls.into_iter().map(|(_, k)| k).collect();
        let mut rel = Rel::Aggregate {
            input: Box::new(pre_rel),
            group: group.clone(),
            calls: agg_calls,
            fields,
        };
        // Converts an expression over the aggregate's output: group keys and calls become
        // references; anything else must be built from them.
        let agg_fields = rel.fields().to_vec();
        let post = |this: &Self, e: &Expr| -> PlanResult<ScalarExpr> {
            this.expr_over_aggregate(&scope, e, &keys, &call_keys, g, &agg_fields)
        };
        if let Some(h) = having {
            let condition = post(self, h)?;
            rel = Rel::Filter {
                input: Box::new(rel),
                condition,
            };
        }
        let mut exprs = vec![];
        let mut names = vec![];
        for (e, name) in items {
            if contains_over(e) {
                return Err(PlanError::unsupported(
                    "OVER aggregates (OverAggregate)",
                    "FC2",
                ));
            }
            exprs.push(post(self, e)?);
            names.push(name.clone());
        }
        Ok((rel, exprs, names))
    }

    fn expr_over_aggregate(
        &self,
        scope: &RelScope,
        e: &Expr,
        keys: &[ScalarExpr],
        call_keys: &[Expr],
        g: usize,
        fields: &[Field],
    ) -> PlanResult<ScalarExpr> {
        if let Some(i) = call_keys.iter().position(|k| k == e) {
            return Ok(ScalarExpr::input(g + i, fields[g + i].ty.clone()));
        }
        if let Expr::Nested(inner) = e {
            return self.expr_over_aggregate(scope, inner, keys, call_keys, g, fields);
        }
        // A whole group expression.
        if let Ok(x) = self.expr(scope, e)
            && let Some(i) = keys.iter().position(|k| *k == x)
        {
            return Ok(ScalarExpr::input(i, fields[i].ty.clone()));
        }
        // Rebuild the expression with the aggregate calls and group keys as placeholders.
        let mut placeholder_scope = scope.clone();
        let base = scope.entries.len();
        let mut substitutes: Vec<ScalarExpr> = (0..base)
            .map(|i| ScalarExpr::input(usize::MAX - i, scope.entries[i].ty.clone()))
            .collect();
        for (i, k) in call_keys.iter().enumerate() {
            let name = format!("$agg{i}");
            placeholder_scope.entries.push(ScopeEntry {
                qualifier: None,
                name: name.clone(),
                ty: fields[g + i].ty.clone(),
            });
            substitutes.push(ScalarExpr::input(g + i, fields[g + i].ty.clone()));
            let _ = k;
        }
        let rewritten = replace_aggregates(e, call_keys);
        let x = self.expr(&placeholder_scope, &rewritten)?;
        let x = x.substitute(&substitutes);
        replace_keys(x, keys, fields)
    }

    // ---------------------------------------------------------------------------------------
    // Expressions
    // ---------------------------------------------------------------------------------------

    fn expr(&self, scope: &RelScope, e: &Expr) -> PlanResult<ScalarExpr> {
        let ty = || self.type_of(scope, e);
        Ok(match e {
            Expr::Identifier(i) => match scope.resolve(None, &i.value) {
                Some(index) => ScalarExpr::input(index, scope.entries[index].ty.clone()),
                None => {
                    // A field of a ROW column (Calcite's PEEK_FIELDS).
                    let t = ty()?;
                    let owner = scope.entries.iter().position(|en| match &en.ty.kind {
                        LogicalKind::Row(fs) => fs.iter().any(|f| f.name == i.value),
                        _ => false,
                    });
                    let Some(owner) = owner else {
                        return Err(PlanError::invalid(format!(
                            "Column '{}' not found in any table",
                            i.value
                        )));
                    };
                    ScalarExpr::call(
                        Op::FieldAccess(i.value.clone()),
                        vec![ScalarExpr::input(owner, scope.entries[owner].ty.clone())],
                        t,
                    )
                }
            },
            Expr::CompoundIdentifier(parts) => {
                let names: Vec<&str> = parts.iter().map(|p| p.value.as_str()).collect();
                let (mut x, rest) = if names.len() >= 2
                    && let Some(index) = scope.resolve(Some(names[0]), names[1])
                {
                    (
                        ScalarExpr::input(index, scope.entries[index].ty.clone()),
                        &names[2..],
                    )
                } else if let Some(index) = scope.resolve(None, names[0]) {
                    (
                        ScalarExpr::input(index, scope.entries[index].ty.clone()),
                        &names[1..],
                    )
                } else {
                    return Err(PlanError::invalid(format!(
                        "Column '{}' not found in any table",
                        names.join(".")
                    )));
                };
                for name in rest {
                    x = field_access(x, name)?;
                }
                x
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                let mut chain = access_chain.as_slice();
                let mut x = match (root.as_ref(), chain.first()) {
                    (Expr::Identifier(q), Some(AccessExpr::Dot(Expr::Identifier(c))))
                        if scope.resolve(None, &q.value).is_none() =>
                    {
                        chain = &chain[1..];
                        let index = scope.resolve(Some(&q.value), &c.value).ok_or_else(|| {
                            PlanError::invalid(format!("Column '{}' not found", c.value))
                        })?;
                        ScalarExpr::input(index, scope.entries[index].ty.clone())
                    }
                    _ => self.expr(scope, root)?,
                };
                for a in chain {
                    x = match a {
                        AccessExpr::Dot(Expr::Identifier(i)) => field_access(x, &i.value)?,
                        AccessExpr::Dot(other) => field_access(x, &other.to_string())?,
                        AccessExpr::Subscript(Subscript::Index { index }) => {
                            let i = self.expr(scope, index)?;
                            let t = match &x.ty().kind {
                                LogicalKind::Array(el) => (**el).clone().nullable(),
                                LogicalKind::Map(_, v) => (**v).clone().nullable(),
                                _ => return Err(PlanError::not_yet("ITEM on a ROW")),
                            };
                            ScalarExpr::call(Op::Item, vec![x, i], t)
                        }
                        AccessExpr::Subscript(Subscript::Slice { .. }) => {
                            return Err(PlanError::not_yet("array slices"));
                        }
                    };
                }
                x
            }
            Expr::Value(v) => self.value(&v.value, ty()?)?,
            Expr::TypedString(t) => {
                let text = match &t.value.value {
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
                    other => other.to_string(),
                };
                let ty = ty()?;
                let value = match &ty.kind {
                    LogicalKind::Date => Literal::Date(
                        parse_date(&text)
                            .ok_or_else(|| PlanError::invalid(format!("Illegal DATE '{text}'")))?,
                    ),
                    LogicalKind::Time(_) => Literal::Time(
                        parse_time(&text)
                            .ok_or_else(|| PlanError::invalid(format!("Illegal TIME '{text}'")))?
                            .0 as i32,
                    ),
                    LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. } => {
                        let (millis, nano_of_milli) = parse_timestamp(&text).ok_or_else(|| {
                            PlanError::invalid(format!("Illegal TIMESTAMP '{text}'"))
                        })?;
                        Literal::Timestamp {
                            millis,
                            nano_of_milli,
                        }
                    }
                    _ => {
                        return Ok(ScalarExpr::call(
                            Op::Cast { try_cast: false },
                            vec![ScalarExpr::literal(
                                Literal::String(text.clone()),
                                LogicalType::char(text.chars().count() as u32).not_null(),
                            )],
                            ty,
                        ));
                    }
                };
                ScalarExpr::literal(value, ty)
            }
            Expr::Interval(i) => {
                let text = match i.value.as_ref() {
                    Expr::Value(v) => match &v.value {
                        Value::SingleQuotedString(s) => s.clone(),
                        Value::Number(n, _) => n.clone(),
                        other => other.to_string(),
                    },
                    Expr::UnaryOp {
                        op: UnaryOperator::Minus,
                        expr,
                    } => format!("-{}", expr.to_string().trim_matches('\'')),
                    other => return Err(PlanError::not_yet(format!("interval of {other}"))),
                };
                let value = interval_value(&text, i.leading_field.as_ref(), i.last_field.as_ref())?;
                ScalarExpr::literal(value, ty()?)
            }
            Expr::Nested(inner) => self.expr(scope, inner)?,
            Expr::BinaryOp { left, op, right } => {
                let l = self.expr(scope, left)?;
                let r = self.expr(scope, right)?;
                let t = ty()?;
                let op = match op {
                    BinaryOperator::Plus => Op::Plus,
                    BinaryOperator::Minus => Op::Minus,
                    BinaryOperator::Multiply => Op::Times,
                    BinaryOperator::Divide => Op::Divide,
                    BinaryOperator::Modulo => Op::Mod,
                    BinaryOperator::Eq => Op::Eq,
                    BinaryOperator::NotEq => Op::NotEq,
                    BinaryOperator::Gt => Op::Gt,
                    BinaryOperator::GtEq => Op::Ge,
                    BinaryOperator::Lt => Op::Lt,
                    BinaryOperator::LtEq => Op::Le,
                    BinaryOperator::And => Op::And,
                    BinaryOperator::Or => Op::Or,
                    BinaryOperator::StringConcat => Op::Function("CONCAT".into()),
                    other => return Err(PlanError::not_yet(format!("operator {other}"))),
                };
                ScalarExpr::call(op, vec![l, r], t)
            }
            Expr::UnaryOp { op, expr } => {
                let x = self.expr(scope, expr)?;
                match op {
                    UnaryOperator::Not => ScalarExpr::call(Op::Not, vec![x], ty()?),
                    UnaryOperator::Plus => x,
                    UnaryOperator::Minus => match x {
                        ScalarExpr::Literal {
                            value: Literal::Integer(v),
                            ty: t,
                        } => ScalarExpr::literal(Literal::Integer(-v), t),
                        ScalarExpr::Literal {
                            value: Literal::Decimal { unscaled, scale },
                            ty: t,
                        } => ScalarExpr::literal(
                            Literal::Decimal {
                                unscaled: -unscaled,
                                scale,
                            },
                            t,
                        ),
                        ScalarExpr::Literal {
                            value: Literal::Double(v),
                            ty: t,
                        } => ScalarExpr::literal(Literal::Double(-v), t),
                        ScalarExpr::Literal {
                            value: Literal::Millis(v),
                            ty: t,
                        } => ScalarExpr::literal(Literal::Millis(-v), t),
                        other => ScalarExpr::call(Op::Negate, vec![other], ty()?),
                    },
                    other => return Err(PlanError::not_yet(format!("operator {other}"))),
                }
            }
            Expr::IsNull(x) => ScalarExpr::call(Op::IsNull, vec![self.expr(scope, x)?], ty()?),
            Expr::IsNotNull(x) => {
                ScalarExpr::call(Op::IsNotNull, vec![self.expr(scope, x)?], ty()?)
            }
            Expr::IsTrue(x) => ScalarExpr::call(Op::IsTrue, vec![self.expr(scope, x)?], ty()?),
            Expr::IsNotTrue(x) => {
                ScalarExpr::call(Op::IsNotTrue, vec![self.expr(scope, x)?], ty()?)
            }
            Expr::IsFalse(x) => ScalarExpr::call(Op::IsFalse, vec![self.expr(scope, x)?], ty()?),
            Expr::IsNotFalse(x) => {
                ScalarExpr::call(Op::IsNotFalse, vec![self.expr(scope, x)?], ty()?)
            }
            Expr::IsUnknown(x) => ScalarExpr::call(Op::IsNull, vec![self.expr(scope, x)?], ty()?),
            Expr::IsNotUnknown(x) => {
                ScalarExpr::call(Op::IsNotNull, vec![self.expr(scope, x)?], ty()?)
            }
            Expr::IsDistinctFrom(a, b) => ScalarExpr::call(
                Op::IsDistinctFrom,
                vec![self.expr(scope, a)?, self.expr(scope, b)?],
                ty()?,
            ),
            Expr::IsNotDistinctFrom(a, b) => ScalarExpr::call(
                Op::IsNotDistinctFrom,
                vec![self.expr(scope, a)?, self.expr(scope, b)?],
                ty()?,
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let x = self.expr(scope, expr)?;
                let lo = self.expr(scope, low)?;
                let hi = self.expr(scope, high)?;
                let t = ty()?;
                let n1 = x.ty().nullable || lo.ty().nullable;
                let n2 = x.ty().nullable || hi.ty().nullable;
                if *negated {
                    ScalarExpr::call(
                        Op::Or,
                        vec![
                            ScalarExpr::call(Op::Lt, vec![x.clone(), lo], bool_type(n1)),
                            ScalarExpr::call(Op::Gt, vec![x, hi], bool_type(n2)),
                        ],
                        t,
                    )
                } else {
                    ScalarExpr::call(
                        Op::And,
                        vec![
                            ScalarExpr::call(Op::Ge, vec![x.clone(), lo], bool_type(n1)),
                            ScalarExpr::call(Op::Le, vec![x, hi], bool_type(n2)),
                        ],
                        t,
                    )
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let x = self.expr(scope, expr)?;
                let t = ty()?;
                let mut terms = vec![];
                for item in list {
                    let v = self.expr(scope, item)?;
                    let n = x.ty().nullable || v.ty().nullable;
                    terms.push(ScalarExpr::call(
                        if *negated { Op::NotEq } else { Op::Eq },
                        vec![x.clone(), v],
                        bool_type(n),
                    ));
                }
                if terms.len() == 1 {
                    terms.remove(0)
                } else {
                    ScalarExpr::call(if *negated { Op::And } else { Op::Or }, terms, t)
                }
            }
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(PlanError::invalid("LIKE ANY is not Flink SQL"));
                }
                let mut args = vec![self.expr(scope, expr)?, self.expr(scope, pattern)?];
                if let Some(esc) = escape_char {
                    let s = esc.to_string().trim_matches('\'').to_owned();
                    args.push(ScalarExpr::literal(
                        Literal::String(s),
                        LogicalType::char(1).not_null(),
                    ));
                }
                let t = ty()?;
                let like = ScalarExpr::call(Op::Like, args, t.clone());
                if *negated {
                    ScalarExpr::call(Op::Not, vec![like], t)
                } else {
                    like
                }
            }
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                ..
            } => {
                let t = ty()?;
                let x = ScalarExpr::call(
                    Op::SimilarTo,
                    vec![self.expr(scope, expr)?, self.expr(scope, pattern)?],
                    t.clone(),
                );
                if *negated {
                    ScalarExpr::call(Op::Not, vec![x], t)
                } else {
                    x
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let t = ty()?;
                let base = operand.as_ref().map(|o| self.expr(scope, o)).transpose()?;
                let mut args = vec![];
                for w in conditions {
                    let c = self.expr(scope, &w.condition)?;
                    let c = match &base {
                        Some(b) => {
                            let n = b.ty().nullable || c.ty().nullable;
                            ScalarExpr::call(Op::Eq, vec![b.clone(), c], bool_type(n))
                        }
                        None => c,
                    };
                    args.push(c);
                    let r = self.expr(scope, &w.result)?;
                    args.push(cast_to(r, &t));
                }
                let otherwise = match else_result {
                    Some(e) => self.expr(scope, e)?,
                    None => ScalarExpr::literal(Literal::Null, t.clone()),
                };
                args.push(cast_to(otherwise, &t));
                ScalarExpr::call(Op::Case, args, t)
            }
            Expr::Cast { kind, expr, .. } => {
                let x = self.expr(scope, expr)?;
                let t = ty()?;
                let try_cast = matches!(kind, CastKind::TryCast | CastKind::SafeCast);
                ScalarExpr::call(Op::Cast { try_cast }, vec![x], t)
            }
            Expr::Extract { field, expr, .. } => {
                let x = self.expr(scope, expr)?;
                ScalarExpr::call(
                    Op::Function("EXTRACT".into()),
                    vec![
                        ScalarExpr::literal(
                            Literal::Symbol(field.to_string().to_uppercase()),
                            LogicalType::new(LogicalKind::Symbol),
                        ),
                        x,
                    ],
                    ty()?,
                )
            }
            Expr::Ceil { expr, field } | Expr::Floor { expr, field } => {
                let name = if matches!(e, Expr::Ceil { .. }) {
                    "CEIL"
                } else {
                    "FLOOR"
                };
                let mut args = vec![self.expr(scope, expr)?];
                match field {
                    sql::CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {}
                    sql::CeilFloorKind::DateTimeField(f) => args.push(ScalarExpr::literal(
                        Literal::Symbol(f.to_string().to_uppercase()),
                        LogicalType::new(LogicalKind::Symbol),
                    )),
                    sql::CeilFloorKind::Scale(v) => {
                        let t = self.type_of(scope, &Expr::value(v.clone()))?;
                        args.push(self.value(v, t)?)
                    }
                }
                ScalarExpr::call(Op::Function(name.into()), args, ty()?)
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![self.expr(scope, expr)?];
                if let Some(f) = substring_from {
                    args.push(self.expr(scope, f)?);
                }
                if let Some(f) = substring_for {
                    args.push(self.expr(scope, f)?);
                }
                ScalarExpr::call(Op::Function("SUBSTRING".into()), args, ty()?)
            }
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                if trim_characters.is_some() {
                    return Err(PlanError::not_yet("TRIM with several characters"));
                }
                let side = match trim_where {
                    Some(sql::TrimWhereField::Leading) => "LEADING",
                    Some(sql::TrimWhereField::Trailing) => "TRAILING",
                    _ => "BOTH",
                };
                let what = match trim_what {
                    Some(w) => self.expr(scope, w)?,
                    None => ScalarExpr::literal(
                        Literal::String(" ".into()),
                        LogicalType::char(1).not_null(),
                    ),
                };
                ScalarExpr::call(
                    Op::Function("TRIM".into()),
                    vec![
                        ScalarExpr::literal(
                            Literal::Symbol(side.into()),
                            LogicalType::new(LogicalKind::Symbol),
                        ),
                        what,
                        self.expr(scope, expr)?,
                    ],
                    ty()?,
                )
            }
            Expr::Position { expr, r#in } => ScalarExpr::call(
                Op::Function("POSITION".into()),
                vec![self.expr(scope, expr)?, self.expr(scope, r#in)?],
                ty()?,
            ),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let mut args = vec![
                    self.expr(scope, expr)?,
                    self.expr(scope, overlay_what)?,
                    self.expr(scope, overlay_from)?,
                ];
                if let Some(f) = overlay_for {
                    args.push(self.expr(scope, f)?);
                }
                ScalarExpr::call(Op::Function("OVERLAY".into()), args, ty()?)
            }
            Expr::Function(f) => self.function(scope, f, ty()?)?,
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                return Err(PlanError::not_yet("subqueries (IN, EXISTS, scalar)"));
            }
            Expr::Array(a) => {
                let mut args = vec![];
                for x in &a.elem {
                    args.push(self.expr(scope, x)?);
                }
                ScalarExpr::call(Op::Function("ARRAY".into()), args, ty()?)
            }
            Expr::Tuple(items) => {
                let mut args = vec![];
                for x in items {
                    args.push(self.expr(scope, x)?);
                }
                ScalarExpr::call(Op::Function("ROW".into()), args, ty()?)
            }
            other => return Err(PlanError::not_yet(format!("expression {other}"))),
        })
    }

    fn value(&self, v: &Value, ty: LogicalType) -> PlanResult<ScalarExpr> {
        let value = match v {
            Value::Null => Literal::Null,
            Value::Boolean(b) => Literal::Boolean(*b),
            Value::Number(n, _) => match &ty.kind {
                LogicalKind::Int | LogicalKind::BigInt => {
                    Literal::Integer(n.parse().map_err(|_| {
                        PlanError::invalid(format!("Numeric literal '{n}' out of range"))
                    })?)
                }
                LogicalKind::Decimal { .. } => {
                    let (unscaled, scale) = decimal_literal(n)
                        .ok_or_else(|| PlanError::invalid(format!("Illegal number '{n}'")))?;
                    Literal::Decimal { unscaled, scale }
                }
                _ => Literal::Double(
                    n.parse()
                        .map_err(|_| PlanError::invalid(format!("Illegal number '{n}'")))?,
                ),
            },
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::EscapedStringLiteral(s)
            | Value::UnicodeStringLiteral(s)
            | Value::NationalStringLiteral(s) => Literal::String(s.clone()),
            Value::HexStringLiteral(s)
            | Value::SingleQuotedByteStringLiteral(s)
            | Value::DoubleQuotedByteStringLiteral(s) => {
                let bytes = (0..s.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&s[i..(i + 2).min(s.len())], 16))
                    .collect::<Result<Vec<u8>, _>>()
                    .map_err(|_| PlanError::invalid(format!("Illegal binary literal '{s}'")))?;
                Literal::Binary(bytes)
            }
            other => return Err(PlanError::not_yet(format!("literal {other}"))),
        };
        Ok(ScalarExpr::literal(value, ty))
    }

    fn function(
        &self,
        scope: &RelScope,
        f: &sql::Function,
        ty: LogicalType,
    ) -> PlanResult<ScalarExpr> {
        let name = function_name(f);
        if f.over.is_some() {
            return Err(PlanError::unsupported(
                "OVER aggregates (OverAggregate)",
                "FC2",
            ));
        }
        if agg_func(&name, is_star(f)).is_some() {
            return Err(PlanError::invalid(format!(
                "Aggregate function {name} outside of an aggregation"
            )));
        }
        let mut args = vec![];
        for a in function_args(f) {
            match arg_expr(a) {
                Some(Expr::Identifier(i))
                    if scope.resolve(None, &i.value).is_none()
                        && is_symbol_argument(&name, &i.value) =>
                {
                    args.push(ScalarExpr::literal(
                        Literal::Symbol(i.value.to_uppercase()),
                        LogicalType::new(LogicalKind::Symbol),
                    ));
                }
                Some(x) => args.push(self.expr(scope, x)?),
                None => return Err(PlanError::not_yet(format!("arguments of {name}"))),
            }
        }
        let op = match name.as_str() {
            "PROCTIME" => Op::Proctime,
            "MOD" => Op::Mod,
            _ => Op::Function(name),
        };
        Ok(ScalarExpr::call(op, args, ty))
    }
}

/// Function arguments that are keywords rather than columns (`TIMESTAMPADD(DAY, ...)`).
fn is_symbol_argument(function: &str, arg: &str) -> bool {
    matches!(
        function,
        "TIMESTAMPADD" | "TIMESTAMPDIFF" | "TIMESTAMP_DIFF"
    ) && matches!(
        arg.to_uppercase().as_str(),
        "YEAR"
            | "QUARTER"
            | "MONTH"
            | "WEEK"
            | "DAY"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "MILLISECOND"
            | "MICROSECOND"
            | "NANOSECOND"
    )
}

fn const_i64(e: &Expr) -> PlanResult<i64> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n
                .parse()
                .map_err(|_| PlanError::invalid(format!("Illegal LIMIT {n}"))),
            other => Err(PlanError::invalid(format!("Illegal LIMIT {other}"))),
        },
        other => Err(PlanError::not_yet(format!("LIMIT {other}"))),
    }
}

fn contains_non_literal(e: &ScalarExpr) -> bool {
    match e {
        ScalarExpr::Literal { .. } => false,
        ScalarExpr::Input { .. } => true,
        ScalarExpr::Call { op, args, .. } => {
            !matches!(op, Op::Cast { .. } | Op::Function(_) | Op::Negate)
                || args.iter().any(contains_non_literal)
        }
    }
}

fn field_expr(e: &ScopeEntry) -> Expr {
    let ident = sql::Ident::with_quote('`', &e.name);
    match &e.qualifier {
        Some(q) => Expr::CompoundIdentifier(vec![sql::Ident::with_quote('`', q), ident]),
        None => Expr::Identifier(ident),
    }
}

fn default_name(e: &Expr, i: usize) -> String {
    match e {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| format!("EXPR${i}")),
        Expr::Nested(inner) => default_name(inner, i),
        Expr::CompoundFieldAccess { access_chain, .. } => match access_chain.last() {
            Some(AccessExpr::Dot(Expr::Identifier(id))) => id.value.clone(),
            _ => format!("EXPR${i}"),
        },
        _ => format!("EXPR${i}"),
    }
}

/// Makes names unique as Calcite does (`name0`, `name1`, ...).
fn unique_names(names: Vec<String>) -> Vec<String> {
    let mut seen = BTreeMap::<String, usize>::new();
    names
        .into_iter()
        .map(|n| {
            let count = seen.entry(n.clone()).or_insert(0);
            let out = if *count > 0 {
                format!("{n}{}", *count - 1)
            } else {
                n
            };
            *count += 1;
            out
        })
        .collect()
}

fn alias_from(from: From, alias: Option<&sql::TableAlias>) -> From {
    let Some(alias) = alias else { return from };
    let mut scope = from.scope.requalify(Some(&alias.name.value));
    let mut rel = from.rel;
    if !alias.columns.is_empty() {
        for (i, c) in alias.columns.iter().enumerate() {
            if let Some(e) = scope.entries.get_mut(i) {
                e.name = c.name.value.clone();
            }
        }
        let refs = rel.field_refs();
        let names = scope.entries.iter().map(|e| e.name.clone()).collect();
        rel = rel.project(refs, names);
    }
    From { rel, scope }
}

fn is_window_tvf(factor: &TableFactor) -> bool {
    matches!(factor, TableFactor::TableFunction { expr: Expr::Function(f), .. }
        if matches!(function_name(f).as_str(), "TUMBLE" | "HOP" | "CUMULATE" | "SESSION"))
}

fn field_access(x: ScalarExpr, name: &str) -> PlanResult<ScalarExpr> {
    let LogicalKind::Row(fields) = &x.ty().kind else {
        return Err(PlanError::invalid(format!(
            "Column '{name}' not found in any table"
        )));
    };
    let Some(field) = fields.iter().find(|f| f.name == name) else {
        return Err(PlanError::invalid(format!("Unknown field '{name}'")));
    };
    let ty = field
        .ty
        .clone()
        .with_nullable(field.ty.nullable || x.ty().nullable);
    Ok(ScalarExpr::call(Op::FieldAccess(name.into()), vec![x], ty))
}

fn push_unique(pre: &mut Vec<ScalarExpr>, names: &mut Vec<String>, x: ScalarExpr) -> usize {
    if let Some(i) = pre.iter().position(|p| *p == x) {
        return i;
    }
    names.push(format!("$f{}", pre.len()));
    pre.push(x);
    pre.len() - 1
}

/// Replaces each aggregate call of `e` found in `call_keys` with the identifier `$agg{i}`.
fn replace_aggregates(e: &Expr, call_keys: &[Expr]) -> Expr {
    let mut e = e.clone();
    let _ = sql::visit_expressions_mut(&mut e, |x: &mut Expr| {
        if let Some(i) = call_keys.iter().position(|k| k == x) {
            *x = Expr::Identifier(sql::Ident::new(format!("$agg{i}")));
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    e
}

/// Replaces sub-expressions equal to a group key with references to the aggregate's output, and
/// fails if a pre-aggregation field is still referenced.
fn replace_keys(x: ScalarExpr, keys: &[ScalarExpr], fields: &[Field]) -> PlanResult<ScalarExpr> {
    // Placeholders for the pre-aggregation fields use indexes counting down from usize::MAX.
    let unplaced = |x: &ScalarExpr| -> ScalarExpr {
        x.map_inputs(&|i| {
            if i > usize::MAX / 2 {
                usize::MAX - i
            } else {
                i
            }
        })
    };
    fn walk(
        x: &ScalarExpr,
        keys: &[ScalarExpr],
        fields: &[Field],
        unplaced: &dyn Fn(&ScalarExpr) -> ScalarExpr,
    ) -> PlanResult<ScalarExpr> {
        let mut has_placeholder = false;
        let mut inputs = vec![];
        x.inputs(&mut inputs);
        if inputs.iter().any(|i| *i > usize::MAX / 2) {
            has_placeholder = true;
        }
        if has_placeholder {
            let original = unplaced(x);
            let only_base = inputs.iter().all(|i| *i > usize::MAX / 2);
            if only_base && let Some(k) = keys.iter().position(|k| *k == original) {
                return Ok(ScalarExpr::input(k, fields[k].ty.clone()));
            }
        }
        match x {
            ScalarExpr::Input { index, .. } if *index > usize::MAX / 2 => Err(PlanError::invalid(
                "Expression is not being grouped".to_string(),
            )),
            ScalarExpr::Call { op, args, ty } => Ok(ScalarExpr::call(
                op.clone(),
                args.iter()
                    .map(|a| walk(a, keys, fields, unplaced))
                    .collect::<PlanResult<_>>()?,
                ty.clone(),
            )),
            other => Ok(other.clone()),
        }
    }
    walk(&x, keys, fields, &unplaced)
}

/// `e` cast to `ty` unless only nullability or the time-attribute marker differ.
pub fn cast_to(e: ScalarExpr, ty: &LogicalType) -> ScalarExpr {
    let from = materialized(e.ty());
    let to = materialized(ty);
    if from.kind == to.kind {
        if e.ty().kind == ty.kind {
            return e;
        }
        return ScalarExpr::call(Op::Reinterpret, vec![e], ty.clone());
    }
    if matches!(
        e,
        ScalarExpr::Literal {
            value: Literal::Null,
            ..
        }
    ) {
        return ScalarExpr::literal(Literal::Null, ty.clone());
    }
    // CHAR(n) and VARCHAR(n) share one representation; Flink pads nothing at runtime.
    let both_char = matches!(
        (&from.kind, &to.kind),
        (
            LogicalKind::Char(_) | LogicalKind::Varchar(_),
            LogicalKind::Char(_) | LogicalKind::Varchar(_)
        )
    );
    if both_char {
        return ScalarExpr::call(Op::Reinterpret, vec![e], ty.clone());
    }
    let nullable = e.ty().nullable || ty.nullable;
    ScalarExpr::call(
        Op::Cast { try_cast: false },
        vec![e],
        ty.clone().with_nullable(nullable),
    )
}

/// Re-types `e` as `ty` (a computed column's declared type).
fn retype(e: ScalarExpr, ty: &LogicalType) -> ScalarExpr {
    if *e.ty() == *ty {
        return e;
    }
    match e {
        ScalarExpr::Input { index, .. } => ScalarExpr::input(index, ty.clone()),
        ScalarExpr::Call { op, args, ty: t }
            if materialized(&t).kind == materialized(ty).kind || matches!(op, Op::Proctime) =>
        {
            ScalarExpr::call(op, args, ty.clone())
        }
        other => cast_to(other, ty),
    }
}

fn union(l: Rel, r: Rel) -> Rel {
    let fields: Vec<Field> = l
        .fields()
        .iter()
        .zip(r.fields())
        .map(|(a, b)| {
            let ty = loams_stream_sql::types::merging::find_common_type(&[
                materialized(&a.ty),
                materialized(&b.ty),
            ])
            .unwrap_or_else(|| a.ty.clone());
            Field::new(&a.name, ty)
        })
        .collect();
    let conform = |rel: Rel| -> Rel {
        let exprs: Vec<ScalarExpr> = rel
            .field_refs()
            .into_iter()
            .zip(&fields)
            .map(|(e, f)| cast_to(e, &f.ty))
            .collect();
        let names = fields.iter().map(|f| f.name.clone()).collect();
        rel.project(exprs, names)
    };
    let mut inputs = vec![];
    for side in [l, r] {
        match side {
            Rel::Union { inputs: inner, .. } => inputs.extend(inner.into_iter().map(conform)),
            other => inputs.push(conform(other)),
        }
    }
    Rel::Union { inputs, fields }
}

fn distinct(rel: Rel) -> Rel {
    let fields = rel.fields().to_vec();
    let group = (0..fields.len()).collect();
    Rel::Aggregate {
        input: Box::new(rel),
        group,
        calls: vec![],
        fields,
    }
}
