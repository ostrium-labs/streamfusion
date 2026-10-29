//! Printing sqlparser's AST as Flink SQL, and the query checks that run after parsing.
//!
//! sqlparser's `Display` is close to Flink's syntax with three exceptions this module rewrites on a
//! clone before printing: table hints (`t /*+ OPTIONS(...) */`), the place of `FOR SYSTEM_TIME AS
//! OF` (before the alias in Flink, after it in sqlparser), and the dialect's encoded forms (table
//! arguments and `MAP[...]`).

use std::fmt::Write as _;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, Spanned, TableFactor,
    TableVersion, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use sqlparser::tokenizer::Span;

use crate::ast::{Hint, HintKey, HintOptions, Literal};
use crate::cursor::{name_span, quote, raw_ident};
use crate::dialect::{map_constructor, table_argument};
use crate::error::ParseError;
use crate::lexer::HintSite;

/// Prints a query as Flink SQL.
pub fn query_to_string(query: &Query) -> String {
    let mut query = query.clone();
    let _ = query.visit(&mut Printer);
    query.to_string()
}

/// Prints an expression as Flink SQL.
pub fn expr_to_string(expr: &Expr) -> String {
    let mut expr = expr.clone();
    let _ = expr.visit(&mut Printer);
    expr.to_string()
}

struct Printer;

impl VisitorMut for Printer {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name,
            alias,
            with_hints,
            version,
            ..
        } = factor
        {
            if with_hints.is_empty() && version.is_none() {
                return ControlFlow::Continue(());
            }
            let mut text = name.to_string();
            let hints = decode_hints(with_hints);
            if !hints.is_empty() {
                text.push(' ');
                text.push_str(&hints_to_string(&hints));
            }
            if let Some(TableVersion::ForSystemTimeAsOf(expr)) = version {
                let _ = write!(text, " FOR SYSTEM_TIME AS OF {}", expr_to_string(expr));
            }
            if let Some(alias) = alias {
                let _ = write!(text, " {alias}");
            }
            let span = name_span(name);
            *factor = TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(raw_ident(text, span))]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            };
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Some(arg) = table_argument(expr) {
            let mut text = format!("TABLE {}", arg.table);
            match arg.partition_by {
                [] => {}
                [one] => {
                    let _ = write!(text, " PARTITION BY {one}");
                }
                many => {
                    let _ = write!(text, " PARTITION BY ({})", join(many));
                }
            }
            match arg.order_by {
                [] => {}
                [one] => {
                    let _ = write!(text, " ORDER BY {one}");
                }
                many => {
                    let _ = write!(text, " ORDER BY ({})", join(many));
                }
            }
            *expr = Expr::Identifier(raw_ident(text, arg.span));
        } else if let Some(args) = map_constructor(expr) {
            let text = format!("MAP[{}]", join(args));
            *expr = Expr::Identifier(raw_ident(text, expr.span()));
        }
        ControlFlow::Continue(())
    }
}

fn join<T: std::fmt::Display>(items: &[T]) -> String {
    items
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// `/*+ A(...), B */`.
pub(crate) fn hints_to_string(hints: &[Hint]) -> String {
    let body = hints
        .iter()
        .map(hint_to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!("/*+ {body} */")
}

fn hint_to_string(hint: &Hint) -> String {
    let options = match &hint.options {
        HintOptions::None => return hint.name.to_string(),
        HintOptions::Identifiers(ids) => join(ids),
        HintOptions::Literals(literals) => literals
            .iter()
            .map(|l| quote(&l.value))
            .collect::<Vec<_>>()
            .join(", "),
        HintOptions::KeyValues(pairs) => pairs
            .iter()
            .map(|(k, v)| {
                let key = match k {
                    HintKey::Identifier(ident) => ident.to_string(),
                    HintKey::Literal(l) => quote(&l.value),
                };
                format!("{key} = {}", quote(&v.value))
            })
            .collect::<Vec<_>>()
            .join(", "),
    };
    format!("{}({options})", hint.name)
}

fn string_value(literal: &Literal) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(literal.value.clone()),
        span: literal.span,
    })
}

/// Encodes table hints into `TableFactor::Table::with_hints` (one `Function` per hint).
pub(crate) fn encode_hints(hints: &[Hint]) -> Vec<Expr> {
    hints
        .iter()
        .map(|hint| {
            let args = match &hint.options {
                HintOptions::None => None,
                HintOptions::Identifiers(ids) => Some(
                    ids.iter()
                        .cloned()
                        .map(Expr::Identifier)
                        .collect::<Vec<_>>(),
                ),
                HintOptions::Literals(literals) => {
                    Some(literals.iter().map(string_value).collect())
                }
                HintOptions::KeyValues(pairs) => Some(
                    pairs
                        .iter()
                        .map(|(k, v)| Expr::BinaryOp {
                            left: Box::new(match k {
                                HintKey::Identifier(ident) => Expr::Identifier(ident.clone()),
                                HintKey::Literal(l) => string_value(l),
                            }),
                            op: BinaryOperator::Eq,
                            right: Box::new(string_value(v)),
                        })
                        .collect(),
                ),
            };
            Expr::Function(Function {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                    value: hint.name.value.clone(),
                    quote_style: hint.name.quote_style,
                    span: hint.span,
                })]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: match args {
                    None => FunctionArguments::None,
                    Some(args) => FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: args
                            .into_iter()
                            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                            .collect(),
                        clauses: vec![],
                    }),
                },
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            })
        })
        .collect()
}

fn literal_of(expr: &Expr) -> Option<Literal> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            span,
        }) => Some(Literal {
            value: s.clone(),
            span: *span,
        }),
        _ => None,
    }
}

/// Decodes the table hints of a `TableFactor::Table` (the inverse of [`encode_hints`]).
pub fn decode_hints(with_hints: &[Expr]) -> Vec<Hint> {
    with_hints
        .iter()
        .filter_map(|expr| {
            let Expr::Function(function) = expr else {
                return None;
            };
            let [ObjectNamePart::Identifier(name)] = &function.name.0[..] else {
                return None;
            };
            let args: Vec<&Expr> = match &function.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })
                    .collect(),
                _ => vec![],
            };
            let options = match args.first() {
                None => HintOptions::None,
                Some(Expr::BinaryOp { .. }) => HintOptions::KeyValues(
                    args.iter()
                        .filter_map(|e| match e {
                            Expr::BinaryOp { left, right, .. } => {
                                let key = match left.as_ref() {
                                    Expr::Identifier(ident) => HintKey::Identifier(ident.clone()),
                                    other => HintKey::Literal(literal_of(other)?),
                                };
                                Some((key, literal_of(right)?))
                            }
                            _ => None,
                        })
                        .collect(),
                ),
                Some(Expr::Identifier(_)) => HintOptions::Identifiers(
                    args.iter()
                        .filter_map(|e| match e {
                            Expr::Identifier(ident) => Some(ident.clone()),
                            _ => None,
                        })
                        .collect(),
                ),
                Some(_) => {
                    HintOptions::Literals(args.iter().filter_map(|e| literal_of(e)).collect())
                }
            };
            Some(Hint {
                name: Ident {
                    value: name.value.clone(),
                    quote_style: name.quote_style,
                    span: name.span,
                },
                options,
                span: name.span,
            })
        })
        .collect()
}

/// Attaches table hints to the tables they follow and rejects table-factor syntax Flink lacks.
pub(crate) struct QueryChecker<'s> {
    pub sites: &'s mut [HintSite],
}

impl QueryChecker<'_> {
    /// Hints that follow the object name ending at `end`.
    pub(crate) fn take_hints(&mut self, name: &ObjectName) -> Vec<Hint> {
        let end = name_span(name).end;
        let mut hints = vec![];
        for site in self.sites.iter_mut() {
            if !site.used && site.anchor == end {
                site.used = true;
                hints.extend(site.hints.iter().cloned());
            }
        }
        hints
    }
}

fn reject(what: &str, span: Span) -> ControlFlow<ParseError> {
    ControlFlow::Break(ParseError::syntax(format!("Encountered {what}"), span))
}

impl VisitorMut for QueryChecker<'_> {
    type Break = ParseError;

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<ParseError> {
        // `UNNEST(arr)` comes through the generic table-function path; give it its own node.
        if let TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } = factor
            && name.0.len() == 1
            && matches!(&name.0[0], ObjectNamePart::Identifier(i) if i.value.eq_ignore_ascii_case("UNNEST"))
        {
            let array_exprs = args
                .args
                .iter()
                .filter_map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                    _ => None,
                })
                .collect();
            *factor = TableFactor::UNNEST {
                alias: alias.take(),
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            };
            return ControlFlow::Continue(());
        }
        if let TableFactor::Table {
            name,
            args,
            with_hints,
            version,
            partitions,
            with_ordinality,
            json_path,
            sample,
            index_hints,
            ..
        } = factor
        {
            let span = name_span(name);
            if name.0.iter().any(|part| {
                matches!(part, ObjectNamePart::Identifier(ident) if ident.quote_style == Some('\''))
            }) {
                return reject("a string literal where a table name belongs", span);
            }
            if !with_hints.is_empty() {
                return reject(
                    "WITH (...) table hints (Flink writes /*+ OPTIONS(...) */)",
                    span,
                );
            }
            if args.is_some() {
                // Flink 2.x calls process table functions (FLIP-440) without TABLE(...):
                // `FROM f(r => TABLE t PARTITION BY k)`. Parsed here; the binder names the
                // feature.
                let hints = self.take_hints(name);
                *with_hints = encode_hints(&hints);
                return ControlFlow::Continue(());
            }
            if !partitions.is_empty() || *with_ordinality || json_path.is_some() {
                return reject(
                    "PARTITION, WITH ORDINALITY or a JSON path after a table name",
                    span,
                );
            }
            if sample.is_some() {
                return reject("TABLESAMPLE", span);
            }
            if !index_hints.is_empty() {
                return reject("index hints", span);
            }
            if matches!(version, Some(v) if !matches!(v, TableVersion::ForSystemTimeAsOf(_))) {
                return reject("a table version other than FOR SYSTEM_TIME AS OF", span);
            }
            let hints = self.take_hints(name);
            *with_hints = encode_hints(&hints);
        }
        ControlFlow::Continue(())
    }
}
