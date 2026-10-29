//! Lowering the logical plan to the physical plan: native operator specs and the nodes around
//! them (FC1 Task 10).
//!
//! The shapes follow what Flink's physical planner produces for the same SQL, so `EXPLAIN` and the
//! corpus's operator tags line up: projections and filters merge into one `Calc`, a table's
//! watermark is a `WatermarkAssigner` after its computed columns, and so on.

use loams_stream_sql::types::{LogicalKind, LogicalType};

use crate::changelog::{self, ChangelogMode};
use crate::encode::encode_calc;
use crate::encode::encode_predicate;
use crate::error::{PlanError, PlanResult};
use crate::explain::render_expr;
use crate::expr::{Literal, Op, ScalarExpr};
use crate::expr::{is_proctime, is_rowtime};
use crate::logical::{
    AggCall, AggFunc, Field, JoinKind, RankFunc, Rel, SinkPlan, SortKey, WindowKind,
};
use crate::opspec::{
    GroupAggSpec, KeepFirstSpec, KeepLastSpec, OpSpec, TopNSpec, UpdatingJoinSpec, WindowAggSpec,
    WindowJoinSpec, WindowRankSpec, WindowShape, WindowTvfSpec,
};
use crate::physical::{
    ConnectorTable, Distribution, Edge, NodeId, PhysicalNode, PhysicalOp, PhysicalPlan,
    WatermarkSpec,
};

/// Builds a physical plan.
#[derive(Debug, Default)]
pub struct Lowering {
    plan: PhysicalPlan,
}

/// A projection with an optional filter over a base relation (Calcite's `RexProgram`).
struct Program<'a> {
    base: &'a Rel,
    exprs: Vec<ScalarExpr>,
    names: Vec<String>,
    condition: Option<ScalarExpr>,
}

/// Merges a chain of projections and filters into one program over the first other node.
fn program(rel: &Rel) -> Program<'_> {
    match rel {
        Rel::Project {
            input,
            exprs,
            fields,
        } => {
            let inner = program(input);
            Program {
                base: inner.base,
                exprs: exprs.iter().map(|e| e.substitute(&inner.exprs)).collect(),
                names: fields.iter().map(|f| f.name.clone()).collect(),
                condition: inner.condition,
            }
        }
        Rel::Filter { input, condition } => {
            let inner = program(input);
            let c = condition.substitute(&inner.exprs);
            let condition = match inner.condition {
                Some(prev) => {
                    let mut terms = prev.conjuncts();
                    terms.extend(c.conjuncts());
                    ScalarExpr::and_all(terms)
                }
                None => c,
            };
            Program {
                base: inner.base,
                exprs: inner.exprs,
                names: inner.names,
                condition: Some(condition),
            }
        }
        other => Program {
            base: other,
            exprs: other.field_refs(),
            names: other.fields().iter().map(|f| f.name.clone()).collect(),
            condition: None,
        },
    }
}

fn names(fields: &[Field]) -> String {
    fields
        .iter()
        .map(|f| f.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

impl Lowering {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(self) -> PhysicalPlan {
        self.plan
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        name: &str,
        description: String,
        op: PhysicalOp,
        inputs: Vec<Edge>,
        fields: Vec<Field>,
        changelog: ChangelogMode,
        upsert_key: Option<Vec<usize>>,
    ) -> NodeId {
        let id = self.plan.nodes.len();
        self.plan.nodes.push(PhysicalNode {
            id,
            name: name.to_owned(),
            description,
            op,
            inputs,
            fields,
            changelog,
            upsert_key,
        });
        id
    }

    fn node(&self, id: NodeId) -> &PhysicalNode {
        &self.plan.nodes[id]
    }

    fn forward(id: NodeId) -> Vec<Edge> {
        vec![Edge {
            node: id,
            distribution: Distribution::Forward,
        }]
    }

    /// Lowers an `INSERT`: the query, then the sink.
    pub fn sink(&mut self, sink: &SinkPlan) -> PlanResult<NodeId> {
        let input = self.rel(&sink.input)?;
        let table = ConnectorTable {
            name: format!(
                "{}.{}.{}",
                sink.table.id.catalog, sink.table.id.database, sink.table.id.object
            ),
            options: sink.table.options.clone(),
            fields: sink.table.physical.clone(),
            primary_key: sink.table.primary_key.clone(),
        };
        let mode = self.node(input).changelog;
        let accepts = changelog::sink_accepts(table.connector(), table.format());
        if mode.is_updating() && !accepts.is_updating() {
            return Err(PlanError::invalid(format!(
                "Table sink '{}' doesn't support consuming update changes which is produced by node {}",
                table.name,
                self.node(input).name
            )));
        }
        if mode.is_updating()
            && let Some(pk) = &table.primary_key
        {
            // Flink materializes updates into a keyed sink unless the stream is already keyed by
            // the sink's primary key (`table.exec.sink.upsert-materialize = AUTO`).
            let mut key = self.node(input).upsert_key.clone().unwrap_or_default();
            let mut pk = pk.clone();
            key.sort();
            pk.sort();
            if key.is_empty() || key != pk {
                return Err(PlanError::not_yet(
                    "SinkMaterializer (updates into a keyed sink)",
                ));
            }
        }
        let not_null: Vec<usize> = table
            .fields
            .iter()
            .enumerate()
            .filter(|(i, f)| !f.ty.nullable && self.node(input).fields[*i].ty.nullable)
            .map(|(i, _)| i)
            .collect();
        if !not_null.is_empty() {
            return Err(PlanError::not_yet(
                "ConstraintEnforcer (NOT NULL sink columns)",
            ));
        }
        let description = format!("table=[{}], fields=[{}]", table.name, names(&table.fields));
        let fields = table.fields.clone();
        Ok(self.push(
            "Sink",
            description,
            PhysicalOp::Sink(table),
            Self::forward(input),
            fields,
            mode,
            None,
        ))
    }

    /// Lowers a relation; returns the node producing it.
    pub fn rel(&mut self, rel: &Rel) -> PlanResult<NodeId> {
        match rel {
            Rel::Scan { table, fields } => {
                let t = ConnectorTable {
                    name: format!(
                        "{}.{}.{}",
                        table.id.catalog, table.id.database, table.id.object
                    ),
                    options: table.options.clone(),
                    fields: fields.clone(),
                    primary_key: table.primary_key.clone(),
                };
                let mode = changelog::source_mode(t.connector(), t.format());
                let description = format!(
                    "table=[[{}, {}, {}]], fields=[{}]",
                    table.id.catalog,
                    table.id.database,
                    table.id.object,
                    names(fields)
                );
                let key = t.primary_key.clone();
                Ok(self.push(
                    "TableSourceScan",
                    description,
                    PhysicalOp::Source(t),
                    vec![],
                    fields.clone(),
                    mode,
                    key,
                ))
            }
            Rel::Values { rows, fields } => {
                let mut out = vec![];
                for row in rows {
                    let mut values = vec![];
                    for e in row {
                        values.push(constant(e).ok_or_else(|| {
                            PlanError::not_yet("VALUES of non-constant expressions")
                        })?);
                    }
                    out.push(values);
                }
                let description = format!("tuples=[{} rows]", out.len());
                Ok(self.push(
                    "Values",
                    description,
                    PhysicalOp::Values(out),
                    vec![],
                    fields.clone(),
                    ChangelogMode::INSERT_ONLY,
                    None,
                ))
            }
            Rel::Project { .. } | Rel::Filter { .. } => {
                let mut p = program(rel);
                if let Rel::Rank {
                    rank_end: i64::MAX, ..
                } = p.base
                {
                    let (rank, exprs, condition) = bound_rank(p.base, &p.exprs, p.condition)?;
                    let input = self.rel(&rank)?;
                    p.exprs = exprs;
                    return self.calc(input, &p.exprs, &p.names, condition.as_ref());
                }
                let input = self.rel(p.base)?;
                self.calc(input, &p.exprs, &p.names, p.condition.as_ref())
            }
            Rel::Watermark {
                input,
                rowtime,
                expr,
            } => {
                let delay_ms = watermark_delay(expr, *rowtime)?;
                let input = self.rel(input)?;
                let node = self.node(input);
                let fields = node.fields.clone();
                let mode = node.changelog;
                let key = node.upsert_key.clone();
                let description = format!(
                    "rowtime=[{}], watermark=[{}]",
                    fields[*rowtime].name,
                    render_expr(expr, &fields)
                );
                Ok(self.push(
                    "WatermarkAssigner",
                    description,
                    PhysicalOp::Watermark(WatermarkSpec {
                        rowtime: *rowtime,
                        delay_ms,
                    }),
                    Self::forward(input),
                    fields,
                    mode,
                    key,
                ))
            }
            Rel::Union { inputs, fields } => {
                let mut edges = vec![];
                let mut mode: Option<ChangelogMode> = None;
                for i in inputs {
                    let id = self.rel(i)?;
                    let m = self.node(id).changelog;
                    mode = Some(mode.map_or(m, |x| x.union(m)));
                    edges.push(Edge {
                        node: id,
                        distribution: Distribution::Forward,
                    });
                }
                let description = format!("all=[true], union=[{}]", names(fields));
                Ok(self.push(
                    "Union",
                    description,
                    PhysicalOp::Union,
                    edges,
                    fields.clone(),
                    mode.unwrap_or(ChangelogMode::INSERT_ONLY),
                    None,
                ))
            }
            Rel::WindowTvf {
                input,
                time,
                window,
                fields,
            } => {
                let (shape, offset_ms) = window_shape(window)?;
                let source = self.rel(input)?;
                if self.node(source).changelog.is_updating() {
                    return Err(PlanError::not_yet(
                        "WindowTableFunction over an updating input",
                    ));
                }
                let description = format!(
                    "window=[{}]",
                    window_text(window, &input.fields()[*time].name)
                );
                Ok(self.push(
                    "WindowTableFunction",
                    description,
                    PhysicalOp::Native(Box::new(OpSpec::WindowTvf(WindowTvfSpec {
                        time_column: *time,
                        shape,
                        offset_ms,
                    }))),
                    Self::forward(source),
                    fields.clone(),
                    ChangelogMode::INSERT_ONLY,
                    None,
                ))
            }
            Rel::Aggregate {
                input,
                group,
                calls,
                fields,
            } => {
                if window_input(input) {
                    self.window_aggregate(input, group, calls, fields)
                } else {
                    self.group_aggregate(input, group, calls, fields)
                }
            }
            Rel::Join {
                left,
                right,
                kind,
                condition,
                fields,
            } => self.join(left, right, *kind, condition, fields),
            Rel::Rank {
                input,
                func,
                partition,
                order,
                rank_start,
                rank_end,
                output_rank,
                fields,
            } => {
                if *rank_end == i64::MAX {
                    return Err(PlanError::unsupported(
                        "OVER aggregates (OverAggregate)",
                        "FC2",
                    ));
                }
                if *func != RankFunc::RowNumber {
                    return Err(PlanError::invalid(
                        "RANK() and DENSE_RANK() on streaming tables are not supported by Flink's Top-N",
                    ));
                }
                self.rank(
                    input,
                    partition,
                    order,
                    *rank_start,
                    *rank_end,
                    *output_rank,
                    fields,
                )
            }
            Rel::Sort {
                input,
                keys,
                offset,
                fetch,
            } => {
                let Some(fetch) = fetch else {
                    return Err(PlanError::unsupported(
                        "ORDER BY without LIMIT (TemporalSort, Sort)",
                        "FC2",
                    ));
                };
                // A streaming LIMIT is a global Top-N, ordered or not (`SortLimit`, `Limit`).
                let source = self.rel(input)?;
                let in_node = self.node(source);
                let insert_only = in_node.changelog.is_insert_only();
                let fields = in_node.fields.clone();
                let spec = TopNSpec {
                    partition_columns: vec![],
                    key_timestamp_precisions: vec![],
                    sort: keys
                        .iter()
                        .map(|k| crate::opspec::SortKey {
                            index: k.index,
                            ascending: k.ascending,
                            nulls_first: k.nulls_first,
                        })
                        .collect(),
                    offset: *offset,
                    limit: offset + fetch,
                    rank_end_column: -1,
                    output_rank_number: false,
                    retracting: *offset > 0 || !insert_only,
                    generate_update_before: true,
                    net_diff: false,
                    state_ttl_ms: 0,
                    memory_budget_bytes: -1,
                };
                let (name, mode) = if keys.is_empty() && insert_only && *offset == 0 {
                    ("Limit", ChangelogMode::INSERT_ONLY)
                } else {
                    ("SortLimit", ChangelogMode::ALL)
                };
                let description = format!(
                    "orderBy=[{}], offset=[{offset}], fetch=[{fetch}]",
                    keys.iter()
                        .map(|k| format!(
                            "{} {}",
                            fields[k.index].name,
                            if k.ascending { "ASC" } else { "DESC" }
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                Ok(self.push(
                    name,
                    description,
                    PhysicalOp::Native(Box::new(OpSpec::TopN(spec))),
                    vec![Edge {
                        node: source,
                        distribution: Distribution::Single,
                    }],
                    fields,
                    mode,
                    None,
                ))
            }
        }
    }

    /// A regular join (`Join`): equi-keys from `left = right` conjuncts, the rest as a predicate
    /// over `[left.., right..]`. Time-bounded joins of two rowtime attributes are interval joins in
    /// Flink and are refused by name until they are lowered.
    fn join(
        &mut self,
        left: &Rel,
        right: &Rel,
        kind: JoinKind,
        condition: &ScalarExpr,
        fields: &[Field],
    ) -> PlanResult<NodeId> {
        let left_len = left.fields().len();
        let mut left_keys = vec![];
        let mut right_keys = vec![];
        let mut filter_nulls = vec![];
        let mut rest = vec![];
        for term in condition.clone().conjuncts() {
            if let ScalarExpr::Call { op, args, .. } = &term
                && matches!(op, Op::Eq | Op::IsNotDistinctFrom)
                && let (Some(a), Some(b)) = (args[0].as_input(), args[1].as_input())
                && (a < left_len) != (b < left_len)
                && crate::expr::materialized(args[0].ty()).kind
                    == crate::expr::materialized(args[1].ty()).kind
            {
                let (l, r) = if a < left_len { (a, b) } else { (b, a) };
                left_keys.push(l);
                right_keys.push(r - left_len);
                filter_nulls.push(if matches!(op, Op::Eq) { 1 } else { 0 });
                continue;
            }
            if matches!(
                term,
                ScalarExpr::Literal {
                    value: Literal::Boolean(true),
                    ..
                }
            ) {
                continue;
            }
            rest.push(term);
        }
        let all_fields: Vec<Field> = left
            .fields()
            .iter()
            .chain(right.fields())
            .cloned()
            .collect();
        if let (Some((lws, lwe)), Some((rws, rwe))) =
            (window_bounds(left.fields()), window_bounds(right.fields()))
        {
            let pair = |l: usize, r: usize| {
                left_keys
                    .iter()
                    .zip(&right_keys)
                    .position(|(a, b)| *a == l && *b == r)
            };
            if let (Some(i), Some(j)) = (pair(lws, rws), pair(lwe, rwe)) {
                let keep: Vec<usize> = (0..left_keys.len())
                    .filter(|k| *k != i && *k != j)
                    .collect();
                let lk: Vec<usize> = keep.iter().map(|k| left_keys[*k]).collect();
                let rk: Vec<usize> = keep.iter().map(|k| right_keys[*k]).collect();
                return self.window_join(
                    left,
                    right,
                    kind,
                    condition,
                    fields,
                    (lk, rk),
                    (lws, lwe),
                    (rws, rwe),
                    rest,
                );
            }
        }
        let time_bound = rest.iter().any(|t| {
            let mut ins = vec![];
            t.inputs(&mut ins);
            ins.iter().any(|i| is_rowtime(&all_fields[*i].ty))
                && ins.iter().any(|i| *i < left_len)
                && ins.iter().any(|i| *i >= left_len)
        });
        if time_bound {
            return Err(PlanError::not_yet("IntervalJoin"));
        }
        let l = self.rel(left)?;
        let r = self.rel(right)?;
        let (lmode, rmode) = (self.node(l).changelog, self.node(r).changelog);
        let arrow = |fs: &[Field]| -> PlanResult<Vec<arrow_schema::Field>> {
            fs.iter()
                .map(|f| {
                    loams_stream_sql::types::arrow::to_arrow_field(&f.name, &f.ty)
                        .map_err(|e| PlanError::not_yet(format!("join over {e}")))
                })
                .collect()
        };
        let predicate = if rest.is_empty() {
            None
        } else {
            Some(encode_predicate(&ScalarExpr::and_all(rest.clone()))?)
        };
        let precisions = left_keys
            .iter()
            .map(|k| match left.fields()[*k].ty.kind {
                LogicalKind::Timestamp { precision, .. }
                | LogicalKind::TimestampLtz { precision, .. } => precision as i32,
                _ => -1,
            })
            .collect();
        let join_type = join_code(kind);
        let spec = UpdatingJoinSpec {
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
            join_type,
            left_fields: arrow(left.fields())?,
            right_fields: arrow(right.fields())?,
            predicate,
            key_timestamp_precisions: precisions,
            filter_nulls,
            left_unique: false,
            right_unique: false,
            mini_batch: false,
            left_state_ttl_ms: 0,
            right_state_ttl_ms: 0,
            memory_budget_bytes: -1,
        };
        let mode = if kind == JoinKind::Inner && lmode.is_insert_only() && rmode.is_insert_only() {
            ChangelogMode::INSERT_ONLY
        } else {
            ChangelogMode::ALL
        };
        let dist = |keys: &[usize]| {
            if keys.is_empty() {
                Distribution::Single
            } else {
                Distribution::Hash(keys.to_vec())
            }
        };
        let description = format!(
            "joinType=[{}], where=[{}], select=[{}]",
            match kind {
                JoinKind::Inner => "InnerJoin",
                JoinKind::Left => "LeftOuterJoin",
                JoinKind::Right => "RightOuterJoin",
                JoinKind::Full => "FullOuterJoin",
                JoinKind::Semi => "LeftSemiJoin",
                JoinKind::Anti => "LeftAntiJoin",
            },
            render_expr(condition, &all_fields),
            names(fields)
        );
        Ok(self.push(
            "Join",
            description,
            PhysicalOp::Native(Box::new(OpSpec::UpdatingJoin(spec))),
            vec![
                Edge {
                    node: l,
                    distribution: dist(&left_keys),
                },
                Edge {
                    node: r,
                    distribution: dist(&right_keys),
                },
            ],
            fields.to_vec(),
            mode,
            None,
        ))
    }

    /// A window join: both inputs windowed, joined within equal windows.
    #[allow(clippy::too_many_arguments)]
    fn window_join(
        &mut self,
        left: &Rel,
        right: &Rel,
        kind: JoinKind,
        condition: &ScalarExpr,
        fields: &[Field],
        (left_keys, right_keys): (Vec<usize>, Vec<usize>),
        left_window: (usize, usize),
        right_window: (usize, usize),
        rest: Vec<ScalarExpr>,
    ) -> PlanResult<NodeId> {
        let l = self.rel(left)?;
        let r = self.rel(right)?;
        if self.node(l).changelog.is_updating() || self.node(r).changelog.is_updating() {
            return Err(PlanError::not_yet("WindowJoin over an updating input"));
        }
        let predicate = if rest.is_empty() {
            None
        } else {
            Some(encode_predicate(&ScalarExpr::and_all(rest))?)
        };
        let spec = WindowJoinSpec {
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
            left_window_start: left_window.0,
            left_window_end: left_window.1,
            right_window_start: right_window.0,
            right_window_end: right_window.1,
            join_type: join_code(kind),
            left_fields: arrow_fields(left.fields())?,
            right_fields: arrow_fields(right.fields())?,
            predicate,
            key_timestamp_precisions: vec![],
            memory_budget_bytes: -1,
        };
        let all_fields: Vec<Field> = left
            .fields()
            .iter()
            .chain(right.fields())
            .cloned()
            .collect();
        let description = format!(
            "joinType=[{kind:?}], where=[{}], select=[{}]",
            render_expr(condition, &all_fields),
            names(fields)
        );
        let dist = |keys: &[usize]| {
            if keys.is_empty() {
                Distribution::Single
            } else {
                Distribution::Hash(keys.to_vec())
            }
        };
        Ok(self.push(
            "WindowJoin",
            description,
            PhysicalOp::Native(Box::new(OpSpec::WindowJoin(spec))),
            vec![
                Edge {
                    node: l,
                    distribution: dist(&left_keys),
                },
                Edge {
                    node: r,
                    distribution: dist(&right_keys),
                },
            ],
            fields.to_vec(),
            ChangelogMode::INSERT_ONLY,
            None,
        ))
    }

    /// A bounded `ROW_NUMBER()` rank: `Deduplicate` when it keeps the first or last row per key
    /// by a time attribute, else a Top-N (`Rank`).
    #[allow(clippy::too_many_arguments)]
    fn rank(
        &mut self,
        input: &Rel,
        partition: &[usize],
        order: &[SortKey],
        rank_start: i64,
        rank_end: i64,
        output_rank: bool,
        fields: &[Field],
    ) -> PlanResult<NodeId> {
        let source = self.rel(input)?;
        let in_node = self.node(source);
        let in_fields = in_node.fields.clone();
        let insert_only = in_node.changelog.is_insert_only();
        let precisions: Vec<i32> = partition
            .iter()
            .map(|g| match in_fields[*g].ty.kind {
                LogicalKind::Timestamp { precision, .. }
                | LogicalKind::TimestampLtz { precision, .. } => precision as i32,
                _ => -1,
            })
            .collect();
        let distribution = if partition.is_empty() {
            Distribution::Single
        } else {
            Distribution::Hash(partition.to_vec())
        };
        let edge = vec![Edge {
            node: source,
            distribution,
        }];
        let partition_names = partition
            .iter()
            .map(|i| in_fields[*i].name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        let order_names = order
            .iter()
            .map(|k| {
                format!(
                    "{} {}",
                    in_fields[k.index].name,
                    if k.ascending { "ASC" } else { "DESC" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        if let Some((ws, we)) = window_bounds(&in_fields)
            && partition.contains(&ws)
            && partition.contains(&we)
        {
            if rank_start != 1 {
                return Err(PlanError::not_yet("window Top-N with an offset"));
            }
            if !insert_only {
                return Err(PlanError::not_yet("WindowRank over an updating input"));
            }
            let keys: Vec<usize> = partition
                .iter()
                .copied()
                .filter(|p| *p != ws && *p != we)
                .collect();
            let dedup = order.len() == 1 && rank_end == 1 && !output_rank && {
                let t = &in_fields[order[0].index].ty;
                is_rowtime(t) || is_proctime(t)
            };
            let spec = WindowRankSpec {
                window_start_column: ws,
                window_end_column: we,
                partition_columns: keys,
                sort: order
                    .iter()
                    .map(|k| crate::opspec::SortKey {
                        index: k.index,
                        ascending: k.ascending,
                        nulls_first: k.nulls_first,
                    })
                    .collect(),
                limit: rank_end,
                output_rank_number: output_rank,
                keep_last_on_tie: dedup && !order[0].ascending,
                memory_budget_bytes: -1,
            };
            let name = if dedup {
                "WindowDeduplicate"
            } else {
                "WindowRank"
            };
            let description = format!(
                "window=[window_start, window_end], partitionBy=[{partition_names}], orderBy=[{order_names}], rankEnd=[{rank_end}], select=[{}]",
                names(fields)
            );
            return Ok(self.push(
                name,
                description,
                PhysicalOp::Native(Box::new(OpSpec::WindowRank(spec))),
                edge,
                fields.to_vec(),
                ChangelogMode::INSERT_ONLY,
                None,
            ));
        }
        let time_order = order.len() == 1 && {
            let t = &in_fields[order[0].index].ty;
            is_rowtime(t) || is_proctime(t)
        };
        if time_order && rank_start == 1 && rank_end == 1 && !output_rank {
            if !insert_only {
                return Err(PlanError::not_yet("Deduplicate over an updating input"));
            }
            let key = order[0];
            let proctime = is_proctime(&in_fields[key.index].ty);
            let keep_last = !key.ascending;
            let description = format!(
                "keep=[{}], key=[{partition_names}], order=[{}]",
                if keep_last { "LastRow" } else { "FirstRow" },
                if proctime { "PROCTIME" } else { "ROWTIME" }
            );
            if !proctime && !keep_last {
                let spec = KeepFirstSpec {
                    partition_columns: partition.to_vec(),
                    key_timestamp_precisions: precisions,
                    rowtime_column: key.index,
                    state_ttl_ms: 0,
                    memory_budget_bytes: -1,
                };
                return Ok(self.push(
                    "Deduplicate",
                    description,
                    PhysicalOp::Native(Box::new(OpSpec::KeepFirst(spec))),
                    edge,
                    fields.to_vec(),
                    ChangelogMode::INSERT_ONLY,
                    Some(partition.to_vec()),
                ));
            }
            let spec = KeepLastSpec {
                partition_columns: partition.to_vec(),
                key_timestamp_precisions: precisions,
                rowtime_column: key.index,
                generate_update_before: true,
                generate_insert: true,
                rowtime_ordered: !proctime,
                keep_first: !keep_last,
                mini_batch: false,
                compact_changes: false,
                state_ttl_ms: 0,
                memory_budget_bytes: -1,
            };
            let mode = if keep_last {
                ChangelogMode {
                    delete: false,
                    ..ChangelogMode::ALL
                }
            } else {
                ChangelogMode::INSERT_ONLY
            };
            return Ok(self.push(
                "Deduplicate",
                description,
                PhysicalOp::Native(Box::new(OpSpec::KeepLast(spec))),
                edge,
                fields.to_vec(),
                mode,
                Some(partition.to_vec()),
            ));
        }
        let offset = rank_start - 1;
        let spec = TopNSpec {
            partition_columns: partition.to_vec(),
            key_timestamp_precisions: precisions,
            sort: order
                .iter()
                .map(|k| crate::opspec::SortKey {
                    index: k.index,
                    ascending: k.ascending,
                    nulls_first: k.nulls_first,
                })
                .collect(),
            offset,
            limit: rank_end,
            rank_end_column: -1,
            output_rank_number: output_rank,
            retracting: offset > 0 || !insert_only,
            generate_update_before: true,
            net_diff: false,
            state_ttl_ms: 0,
            memory_budget_bytes: -1,
        };
        let description = format!(
            "rankType=[ROW_NUMBER], rankRange=[rankStart={rank_start}, rankEnd={rank_end}], partitionBy=[{partition_names}], orderBy=[{order_names}], select=[{}]",
            names(fields)
        );
        Ok(self.push(
            "Rank",
            description,
            PhysicalOp::Native(Box::new(OpSpec::TopN(spec))),
            edge,
            fields.to_vec(),
            ChangelogMode::ALL,
            None,
        ))
    }

    /// A non-windowed `GROUP BY` (`GroupAggregate`): keys and arguments come from the input's
    /// fields; the output is the keys, then one column per aggregate, as a retracting changelog.
    fn group_aggregate(
        &mut self,
        input: &Rel,
        group: &[usize],
        calls: &[AggCall],
        fields: &[Field],
    ) -> PlanResult<NodeId> {
        let source = self.rel(input)?;
        let in_node = self.node(source);
        let in_fields = in_node.fields.clone();
        let insert_only = in_node.changelog.is_insert_only();
        let mut kinds = vec![];
        let mut value_types = vec![];
        let mut value_columns = vec![];
        let mut filter_columns = vec![];
        let mut aux_columns = vec![];
        for c in calls {
            let arg_ty = c.args.first().map(|a| in_fields[*a].ty.clone());
            let running = arg_ty.as_ref().is_some_and(|t| {
                matches!(
                    t.kind,
                    LogicalKind::TinyInt
                        | LogicalKind::SmallInt
                        | LogicalKind::Int
                        | LogicalKind::BigInt
                        | LogicalKind::Float
                        | LogicalKind::Double
                        | LogicalKind::Date
                        | LogicalKind::Time(_)
                        | LogicalKind::Boolean
                )
            });
            let kind = match (&c.func, c.distinct) {
                (AggFunc::Count | AggFunc::CountStar, false) => 3,
                (AggFunc::Count, true) => 7,
                (AggFunc::Sum, false) => 0,
                (AggFunc::Sum, true) => 9,
                (AggFunc::Avg, false) => 4,
                (AggFunc::Avg, true) => 17,
                (AggFunc::Sum0, false) => 22,
                (AggFunc::Sum0, true) => 34,
                (AggFunc::Min, _) if insert_only && running => 10,
                (AggFunc::Max, _) if insert_only && running => 11,
                (AggFunc::Min, _) => 1,
                (AggFunc::Max, _) => 2,
                (AggFunc::FirstValue, false) => {
                    if insert_only {
                        12
                    } else {
                        15
                    }
                }
                (AggFunc::LastValue, false) => {
                    if insert_only {
                        13
                    } else {
                        16
                    }
                }
                (AggFunc::FirstValue, true) => 19,
                (AggFunc::LastValue, true) => 20,
                (AggFunc::ListAgg, distinct) => match (insert_only, distinct) {
                    (true, false) => 24,
                    (false, false) => 25,
                    (true, true) => 32,
                    (false, true) => 33,
                },
                (AggFunc::Other(name), false) if name == "SINGLE_VALUE" => 14,
                (other, d) => {
                    return Err(PlanError::not_yet(format!(
                        "group aggregate function {other:?}{}",
                        if d { " DISTINCT" } else { "" }
                    )));
                }
            };
            if c.args.len() > 1 && !matches!(c.func, AggFunc::ListAgg) {
                return Err(PlanError::not_yet("multi-argument group aggregates"));
            }
            let retains = matches!(
                c.func,
                AggFunc::Min | AggFunc::Max | AggFunc::FirstValue | AggFunc::LastValue
            ) || matches!(&c.func, AggFunc::Other(n) if n == "SINGLE_VALUE");
            let code = match &arg_ty {
                None => 0,
                Some(t) if retains => retained_type_code(t),
                Some(t) => {
                    let code = value_type_code(t).unwrap_or(0);
                    if kind == 7 && code == 0 && !matches!(t.kind, LogicalKind::BigInt) {
                        100
                    } else {
                        code
                    }
                }
            };
            kinds.push(kind);
            value_types.push(code);
            value_columns.push(c.args.first().map_or(-1, |a| *a as i64));
            filter_columns.push(c.filter.map_or(-1, |f| f as i64));
            aux_columns.push(match (&c.func, c.args.get(1)) {
                (AggFunc::ListAgg, Some(sep)) => *sep as i64,
                _ => -1,
            });
        }
        let has_aux = aux_columns.iter().any(|c| *c >= 0);
        let key_timestamp_precisions = group
            .iter()
            .map(|g| match in_fields[*g].ty.kind {
                LogicalKind::Timestamp { precision, .. }
                | LogicalKind::TimestampLtz { precision, .. } => precision as i32,
                _ => -1,
            })
            .collect();
        let spec = GroupAggSpec {
            aggregate_kinds: kinds,
            value_types,
            value_columns,
            key_columns: group.to_vec(),
            key_timestamp_precisions,
            filter_columns,
            count_columns: if has_aux { aux_columns } else { vec![] },
            distinct_view_columns: vec![],
            record_count_column: -1,
            generate_update_before: true,
            mini_batch: false,
            state_ttl_ms: 0,
            memory_budget_bytes: -1,
            emit_unchanged_with_ttl: false,
        };
        let description = format!(
            "groupBy=[{}], select=[{}]",
            group
                .iter()
                .map(|g| in_fields[*g].name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            fields
                .iter()
                .take(group.len())
                .map(|f| f.name.clone())
                .chain(calls.iter().map(|c| c.name.clone()))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mode = if insert_only {
            ChangelogMode {
                delete: false,
                ..ChangelogMode::ALL
            }
        } else {
            ChangelogMode::ALL
        };
        let distribution = if group.is_empty() {
            Distribution::Single
        } else {
            Distribution::Hash(group.to_vec())
        };
        Ok(self.push(
            "GroupAggregate",
            description,
            PhysicalOp::Native(Box::new(OpSpec::GroupAgg(spec))),
            vec![Edge {
                node: source,
                distribution,
            }],
            fields.to_vec(),
            mode,
            Some((0..group.len()).collect()),
        ))
    }

    /// A window aggregate over a window TVF: a `Calc` into the native operator's
    /// `[ts, value.., key..]` layout, the `WindowAggregate`, and a `Calc` back to the aggregate's
    /// output (group fields, window bounds as timestamps, then the aggregate results).
    fn window_aggregate(
        &mut self,
        input: &Rel,
        group: &[usize],
        calls: &[AggCall],
        fields: &[Field],
    ) -> PlanResult<NodeId> {
        let p = program(input);
        let Rel::WindowTvf {
            input: tvf_input,
            time,
            window,
            ..
        } = p.base
        else {
            unreachable!("window_input checked the base")
        };
        let width = tvf_input.fields().len();
        let (shape, offset_ms) = window_shape(window)?;
        // Which group fields are the window's own columns (0 start, 1 end, 2 time).
        let window_column = |e: &ScalarExpr| match e {
            ScalarExpr::Input { index, .. } if *index >= width => Some(*index - width),
            ScalarExpr::Call {
                op: Op::Reinterpret,
                args,
                ..
            } => match args[0].as_input() {
                Some(i) if i >= width => Some(i - width),
                _ => None,
            },
            _ => None,
        };
        let mut window_props: Vec<(usize, usize)> = vec![];
        let mut keys: Vec<(usize, ScalarExpr)> = vec![];
        for (pos, g) in group.iter().enumerate() {
            let e = &p.exprs[*g];
            match window_column(e) {
                Some(w) => window_props.push((pos, w)),
                None => keys.push((pos, e.clone())),
            }
        }
        let has = |w: usize| window_props.iter().any(|(_, x)| *x == w);
        if !(has(0) && has(1)) {
            return Err(PlanError::not_yet(
                "window aggregates not grouped by window_start and window_end",
            ));
        }
        let reads_window = |e: &ScalarExpr| {
            let mut ins = vec![];
            e.inputs(&mut ins);
            ins.iter().any(|i| *i >= width)
        };
        if keys.iter().any(|(_, e)| reads_window(e))
            || p.condition.as_ref().is_some_and(reads_window)
        {
            return Err(PlanError::not_yet(
                "window columns used inside a window aggregate",
            ));
        }
        let mut kinds = vec![];
        let mut value_types = vec![];
        let mut values = vec![];
        for c in calls {
            if c.filter.is_some() {
                return Err(PlanError::not_yet("FILTER on window aggregates"));
            }
            let kind = match (&c.func, c.distinct) {
                (AggFunc::Count, true) => 7,
                (_, true) => return Err(PlanError::not_yet(format!("{:?} DISTINCT", c.func))),
                (AggFunc::Sum, _) => 0,
                (AggFunc::Min, _) => 1,
                (AggFunc::Max, _) => 2,
                (AggFunc::Count | AggFunc::CountStar, _) => 3,
                (AggFunc::Avg, _) => 4,
                (AggFunc::Sum0, _) => 22,
                (other, _) => {
                    return Err(PlanError::not_yet(format!(
                        "window aggregate function {other:?}"
                    )));
                }
            };
            let value = match c.args.as_slice() {
                [] => ScalarExpr::literal(Literal::Integer(1), LogicalType::bigint().not_null()),
                [a] => p.exprs[*a].clone(),
                _ => return Err(PlanError::not_yet("multi-argument window aggregates")),
            };
            if reads_window(&value) {
                return Err(PlanError::not_yet(
                    "window columns inside a window aggregate",
                ));
            }
            value_types.push(value_type_code(value.ty()).ok_or_else(|| {
                PlanError::not_yet(format!("window aggregates over {}", value.ty()))
            })?);
            kinds.push(kind);
            values.push(value);
        }
        // Pre-Calc over the TVF's input: [ts, value.., key..].
        let source = self.rel(tvf_input)?;
        let time_ty = tvf_input.fields()[*time].ty.clone();
        let mut pre = vec![ScalarExpr::call(
            Op::FieldAccess("millis".into()),
            vec![ScalarExpr::input(*time, time_ty)],
            LogicalType::bigint().not_null(),
        )];
        let mut pre_names = vec!["ts".to_owned()];
        for (i, v) in values.iter().enumerate() {
            pre.push(v.clone());
            pre_names.push(format!("value{i}"));
        }
        for (j, (_, k)) in keys.iter().enumerate() {
            pre.push(k.clone());
            pre_names.push(format!("key{j}"));
        }
        let pre_node = self.calc(source, &pre, &pre_names, p.condition.as_ref())?;
        if self.node(pre_node).changelog.is_updating() {
            return Err(PlanError::not_yet("window aggregates over updating input"));
        }
        // The native operator: [key.., window_start, window_end, result..].
        let k = keys.len();
        let mut agg_fields: Vec<Field> = keys
            .iter()
            .enumerate()
            .map(|(j, (_, e))| Field::new(format!("key{j}"), e.ty().clone()))
            .collect();
        agg_fields.push(Field::new("window_start", LogicalType::bigint().not_null()));
        agg_fields.push(Field::new("window_end", LogicalType::bigint().not_null()));
        for (i, c) in calls.iter().enumerate() {
            agg_fields.push(Field::new(format!("result{i}"), c.ty.clone()));
        }
        let description = format!(
            "groupBy=[{}], window=[{}], select=[{}]",
            keys.iter()
                .map(|(pos, _)| fields[*pos].name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            window_text(window, &tvf_input.fields()[*time].name),
            calls
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let agg = self.push(
            "WindowAggregate",
            description,
            PhysicalOp::Native(Box::new(OpSpec::WindowAgg(WindowAggSpec {
                shape,
                value_types,
                aggregate_kinds: kinds,
                offset_ms,
                memory_budget_bytes: -1,
            }))),
            vec![Edge {
                node: pre_node,
                distribution: if k == 0 {
                    Distribution::Single
                } else {
                    Distribution::Hash((1 + calls.len()..1 + calls.len() + k).collect())
                },
            }],
            agg_fields.clone(),
            ChangelogMode::INSERT_ONLY,
            None,
        );
        // Post-Calc back to the aggregate's output layout.
        let start = ScalarExpr::input(k, LogicalType::bigint().not_null());
        let end = ScalarExpr::input(k + 1, LogicalType::bigint().not_null());
        let to_ts = |millis: ScalarExpr, ty: LogicalType| {
            ScalarExpr::call(Op::Function(TIMESTAMP_FROM_MILLIS.into()), vec![millis], ty)
        };
        let mut post = vec![];
        for (pos, f) in fields.iter().enumerate().take(group.len()) {
            if let Some((_, w)) = window_props.iter().find(|(p, _)| *p == pos) {
                post.push(match w {
                    0 => to_ts(start.clone(), f.ty.clone()),
                    1 => to_ts(end.clone(), f.ty.clone()),
                    _ => to_ts(
                        ScalarExpr::call(
                            Op::Minus,
                            vec![
                                end.clone(),
                                ScalarExpr::literal(
                                    Literal::Integer(1),
                                    LogicalType::bigint().not_null(),
                                ),
                            ],
                            LogicalType::bigint().not_null(),
                        ),
                        f.ty.clone(),
                    ),
                });
            } else {
                let j = keys.iter().position(|(p, _)| *p == pos).expect("a key");
                post.push(ScalarExpr::input(j, agg_fields[j].ty.clone()));
            }
        }
        for (i, f) in fields.iter().enumerate().skip(group.len()) {
            let c = i - group.len();
            let _ = f;
            post.push(ScalarExpr::input(
                k + 2 + c,
                agg_fields[k + 2 + c].ty.clone(),
            ));
        }
        let names: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
        self.calc(agg, &post, &names, None)
    }

    /// A `Calc` over `input`, or `input` itself for an identity projection without a filter.
    fn calc(
        &mut self,
        input: NodeId,
        exprs: &[ScalarExpr],
        names_: &[String],
        condition: Option<&ScalarExpr>,
    ) -> PlanResult<NodeId> {
        let node = self.node(input);
        let identity = condition.is_none()
            && exprs.len() == node.fields.len()
            && exprs
                .iter()
                .enumerate()
                .all(|(i, e)| e.as_input() == Some(i));
        if identity {
            return Ok(input);
        }
        let in_fields = node.fields.clone();
        let mode = node.changelog;
        let in_key = node.upsert_key.clone();
        let spec = encode_calc(exprs, names_, condition)?;
        let mut description = format!(
            "select=[{}]",
            exprs
                .iter()
                .zip(names_)
                .map(|(e, n)| {
                    let r = render_expr(e, &in_fields);
                    if r == *n { r } else { format!("{r} AS {n}") }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        if let Some(c) = condition {
            description.push_str(&format!(", where=[{}]", render_expr(c, &in_fields)));
        }
        let fields: Vec<Field> = exprs
            .iter()
            .zip(names_)
            .map(|(e, n)| Field::new(n, e.ty().clone()))
            .collect();
        // The upsert key survives when every key field is projected unchanged.
        let key = in_key.and_then(|k| {
            k.iter()
                .map(|i| exprs.iter().position(|e| e.as_input() == Some(*i)))
                .collect::<Option<Vec<usize>>>()
        });
        Ok(self.push(
            "Calc",
            description,
            PhysicalOp::Native(Box::new(OpSpec::Calc(spec))),
            Self::forward(input),
            fields,
            mode,
            key,
        ))
    }
}

/// Whether `rel` is a window TVF, possibly under projections and filters.
fn window_input(rel: &Rel) -> bool {
    matches!(program(rel).base, Rel::WindowTvf { .. })
}

/// A literal's value, if `e` is constant.
fn constant(e: &ScalarExpr) -> Option<Literal> {
    match e {
        ScalarExpr::Literal { value, .. } => Some(value.clone()),
        ScalarExpr::Call {
            op: Op::Reinterpret,
            args,
            ..
        } => constant(&args[0]),
        _ => None,
    }
}

/// The delay of `rowtime - INTERVAL`, or 0 for `rowtime` itself.
fn watermark_delay(expr: &ScalarExpr, rowtime: usize) -> PlanResult<i64> {
    let unsupported = || {
        PlanError::not_yet(format!(
            "watermark expression other than `rowtime - INTERVAL` ({})",
            render_expr(expr, &[])
        ))
    };
    let is_rowtime = |e: &ScalarExpr| match e {
        ScalarExpr::Input { index, .. } => *index == rowtime,
        ScalarExpr::Call {
            op: Op::Reinterpret | Op::Cast { .. },
            args,
            ..
        } => args[0].as_input() == Some(rowtime),
        _ => false,
    };
    if is_rowtime(expr) {
        return Ok(0);
    }
    match expr {
        ScalarExpr::Call {
            op: Op::Minus,
            args,
            ..
        } if is_rowtime(&args[0]) => match &args[1] {
            ScalarExpr::Literal {
                value: Literal::Millis(ms),
                ty:
                    LogicalType {
                        kind: LogicalKind::IntervalDayTime { .. },
                        ..
                    },
            } if *ms >= 0 => Ok(*ms),
            _ => Err(unsupported()),
        },
        _ => Err(unsupported()),
    }
}

/// The function the encoder writes as the engine's millisecond-to-timestamp call.
pub const TIMESTAMP_FROM_MILLIS: &str = "$TIMESTAMP_FROM_MILLIS";

/// The engine's value-type code of an aggregate argument (`windowValueTypeCode`).
pub fn value_type_code(ty: &LogicalType) -> Option<i64> {
    Some(match &ty.kind {
        LogicalKind::BigInt => 0,
        LogicalKind::Double => 1,
        LogicalKind::Int => 2,
        LogicalKind::Char(_) | LogicalKind::Varchar(_) => 3,
        LogicalKind::SmallInt => 4,
        LogicalKind::TinyInt => 5,
        LogicalKind::Float => 6,
        LogicalKind::Timestamp { precision, .. } | LogicalKind::TimestampLtz { precision, .. } => {
            1000 + *precision as i64
        }
        LogicalKind::Date => 8,
        LogicalKind::Time(_) => 9,
        LogicalKind::Boolean => 10,
        LogicalKind::Decimal { precision, scale } => 2000 + *precision as i64 * 100 + *scale as i64,
        _ => return None,
    })
}

fn window_text(window: &WindowKind, time: &str) -> String {
    match window {
        WindowKind::Tumble { size_ms, .. } => {
            format!("TUMBLE(time_col=[{time}], size=[{size_ms} ms])")
        }
        WindowKind::Hop {
            size_ms, slide_ms, ..
        } => format!("HOP(time_col=[{time}], size=[{size_ms} ms], slide=[{slide_ms} ms])"),
        WindowKind::Cumulate {
            max_size_ms,
            step_ms,
            ..
        } => {
            format!("CUMULATE(time_col=[{time}], max_size=[{max_size_ms} ms], step=[{step_ms} ms])")
        }
        WindowKind::Session { gap_ms } => format!("SESSION(time_col=[{time}], gap=[{gap_ms} ms])"),
    }
}

/// The value-type code of a value a MIN/MAX/FIRST_VALUE/LAST_VALUE keeps
/// (`retainedValueTypeCode`).
fn retained_type_code(ty: &LogicalType) -> i64 {
    match &ty.kind {
        LogicalKind::IntervalYearMonth { .. }
        | LogicalKind::IntervalDayTime { .. }
        | LogicalKind::Binary(_)
        | LogicalKind::Varbinary(_)
        | LogicalKind::Array(_)
        | LogicalKind::Map(..)
        | LogicalKind::Row(_)
        | LogicalKind::Multiset(_) => 100,
        LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. } => 7,
        _ => value_type_code(ty).unwrap_or(0),
    }
}

/// Folds the conditions on an unbounded rank's number (`rn <= N`, `rn = 1`, `rn BETWEEN a AND b`)
/// into its range, as Flink's `FlinkLogicalRankRule` does, and drops the rank number when nothing
/// else reads it. Returns the bounded rank and the program rewritten over it.
fn bound_rank(
    rank: &Rel,
    exprs: &[ScalarExpr],
    condition: Option<ScalarExpr>,
) -> PlanResult<(Rel, Vec<ScalarExpr>, Option<ScalarExpr>)> {
    let Rel::Rank {
        input,
        func,
        partition,
        order,
        fields,
        ..
    } = rank
    else {
        unreachable!("a rank")
    };
    let r = fields.len() - 1;
    let mut start = 1i64;
    let mut end = i64::MAX;
    let mut rest = vec![];
    for term in condition.map(ScalarExpr::conjuncts).unwrap_or_default() {
        let bound = match &term {
            ScalarExpr::Call { op, args, .. } if args.len() == 2 => {
                let lit = |e: &ScalarExpr| match e {
                    ScalarExpr::Literal {
                        value: Literal::Integer(v),
                        ..
                    } => Some(*v),
                    _ => None,
                };
                match (
                    args[0].as_input(),
                    lit(&args[1]),
                    lit(&args[0]),
                    args[1].as_input(),
                ) {
                    (Some(i), Some(v), _, _) if i == r => Some((op.clone(), v)),
                    (_, _, Some(v), Some(i)) if i == r => Some((
                        match op {
                            Op::Lt => Op::Gt,
                            Op::Le => Op::Ge,
                            Op::Gt => Op::Lt,
                            Op::Ge => Op::Le,
                            other => other.clone(),
                        },
                        v,
                    )),
                    _ => None,
                }
            }
            _ => None,
        };
        match bound {
            Some((Op::Le, v)) => end = end.min(v),
            Some((Op::Lt, v)) => end = end.min(v - 1),
            Some((Op::Eq, v)) => {
                start = start.max(v);
                end = end.min(v);
            }
            Some((Op::Ge, v)) => start = start.max(v),
            Some((Op::Gt, v)) => start = start.max(v + 1),
            _ => rest.push(term),
        }
    }
    if end == i64::MAX {
        return Err(PlanError::unsupported(
            "OVER aggregates (OverAggregate)",
            "FC2",
        ));
    }
    let residual = (!rest.is_empty()).then(|| ScalarExpr::and_all(rest));
    let mut used = vec![];
    for e in exprs.iter().chain(residual.iter()) {
        e.inputs(&mut used);
    }
    let output_rank = used.contains(&r);
    let mut out_fields = fields.clone();
    if !output_rank {
        out_fields.pop();
    }
    let bounded = Rel::Rank {
        input: input.clone(),
        func: *func,
        partition: partition.clone(),
        order: order.clone(),
        rank_start: start,
        rank_end: end,
        output_rank,
        fields: out_fields,
    };
    Ok((bounded, exprs.to_vec(), residual))
}

/// The engine's window shape and offset of a window TVF (sessions are not a shape).
fn window_shape(window: &WindowKind) -> PlanResult<(WindowShape, i64)> {
    Ok(match *window {
        WindowKind::Tumble { size_ms, offset_ms } => (WindowShape::Tumble { size_ms }, offset_ms),
        WindowKind::Hop {
            size_ms,
            slide_ms,
            offset_ms,
        } => (WindowShape::Hop { size_ms, slide_ms }, offset_ms),
        WindowKind::Cumulate {
            max_size_ms,
            step_ms,
            offset_ms,
        } => (
            WindowShape::Cumulate {
                max_size_ms,
                step_ms,
            },
            offset_ms,
        ),
        WindowKind::Session { .. } => return Err(PlanError::not_yet("SESSION windows")),
    })
}

/// The positions of the `window_start` and `window_end` fields of a windowed relation.
fn window_bounds(fields: &[Field]) -> Option<(usize, usize)> {
    let start = fields.iter().position(|f| f.name == "window_start")?;
    let end = fields.iter().position(|f| f.name == "window_end")?;
    Some((start, end))
}

fn join_code(kind: JoinKind) -> i32 {
    match kind {
        JoinKind::Inner => 0,
        JoinKind::Left => 1,
        JoinKind::Right => 2,
        JoinKind::Full => 3,
        JoinKind::Semi => 4,
        JoinKind::Anti => 5,
    }
}

/// The Arrow fields of a relation's fields, in the engine's layout.
fn arrow_fields(fields: &[Field]) -> PlanResult<Vec<arrow_schema::Field>> {
    fields
        .iter()
        .map(|f| {
            loams_stream_sql::types::arrow::to_arrow_field(&f.name, &f.ty)
                .map_err(|e| PlanError::not_yet(format!("join over {e}")))
        })
        .collect()
}
