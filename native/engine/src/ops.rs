//! Native operators built from plain parameters, without Flink or JNI (FC1 Task 2).
//!
//! StreamFusion's Java plugin matches Flink ExecNodes and constructs each native operator through
//! a JNI call carrying its parameters. This module takes those same parameters as Rust structs
//! (`*Params`), and both paths build operators through the functions here, so an operator driven
//! by Loams Stream's runtime is the operator the JNI path runs. [`build`] wraps an operator in
//! [`NativeOperator`], the contract a runtime drives:
//!
//! - **input batches** are Arrow record batches in the operator's column layout (documented on
//!   each `*Params`), with Flink's row kind in the `$row_kind$` column (`Int8`: 0 `+I`, 1 `-U`,
//!   2 `+U`, 3 `-D`) where the operator consumes a changelog;
//! - **event time** advances by [`NativeOperator::watermark`], which returns the rows the
//!   watermark releases (closed windows, expired joins);
//! - **processing time** is the `now_ms` argument of every call;
//! - **end of input or of a mini-batch** is [`NativeOperator::flush`];
//! - **keys** are per row: operators hash and group by their key columns themselves, and Flink's
//!   key group of a key is `crate::flink_key`'s, so keyed state partitions as Flink partitions it.
//!
//! Operators the Java plugin finishes on the JVM side (lookup joins, the watermark assigner, the
//! mini-batch assigner) have no native constructor and are not here.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;

use crate::calc::CalcExpression;
use crate::dedup::{KeepFirstDeduplicator, KeepLastDeduplicator};
use crate::group_agg::GroupAggregator;
use crate::interval_join::IntervalJoiner;
use crate::join_common::{JoinKind, JoinPredicate};
use crate::topn::{RetractableTopNRanker, SortColumn, TopNHandle, TopNRanker};
use crate::window_agg::TumblingAggregator;

/// An expression tree in the engine's prefix encoding: node `i` has kind `kinds[i]`, payload
/// `payload[i]` (an index into `longs`, `doubles` or `strings`, or a column), and
/// `child_counts[i]` children that follow it. This is the encoding the Java plugin writes from
/// Calcite's `RexNode`s; Loams Stream's planner writes it from bound Flink SQL.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EncodedExpr {
    pub kinds: Vec<i64>,
    pub payload: Vec<i64>,
    pub child_counts: Vec<i64>,
    pub longs: Vec<i64>,
    pub doubles: Vec<f64>,
    pub strings: Vec<Option<String>>,
}

/// `Calc`: projections and an optional filter over one input batch.
#[derive(Debug, Clone, PartialEq)]
pub struct CalcParams {
    pub expr: EncodedExpr,
    /// The root node of each output column.
    pub projection_roots: Vec<usize>,
    /// The root node of the filter, or -1 for none.
    pub condition_root: i64,
    pub output_names: Vec<String>,
}

/// The window of a window aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowShape {
    Tumble { size_ms: i64 },
    Hop { size_ms: i64, slide_ms: i64 },
    Cumulate { max_size_ms: i64, step_ms: i64 },
}

/// A window aggregate (`WindowAggregate` over `TUMBLE`, `HOP` or `CUMULATE`). Input columns:
/// `ts` (`Int64` epoch milliseconds of the time attribute), `value{i}` per aggregate and
/// `key{j}` per grouping key; output: keys, `window_start`, `window_end`, then one column per
/// aggregate. Value-type and aggregate-kind codes are the engine's (`aggregates.rs`).
#[derive(Debug, Clone, PartialEq)]
pub struct WindowAggParams {
    pub shape: WindowShape,
    pub value_types: Vec<i64>,
    pub aggregate_kinds: Vec<i64>,
    /// The windows' offset from the epoch (`TUMBLE(..., OFFSET => ...)`), in milliseconds.
    pub offset_ms: i64,
    pub memory_budget_bytes: i64,
}

/// A non-windowed `GROUP BY` aggregate with retraction (`GroupAggregate`).
#[derive(Debug, Clone, PartialEq)]
pub struct GroupAggParams {
    pub aggregate_kinds: Vec<i64>,
    pub value_types: Vec<i64>,
    pub value_columns: Vec<i64>,
    pub key_columns: Vec<usize>,
    pub key_timestamp_precisions: Vec<i32>,
    pub filter_columns: Vec<i64>,
    pub count_columns: Vec<i64>,
    pub distinct_view_columns: Vec<i64>,
    pub record_count_column: i64,
    pub generate_update_before: bool,
    pub mini_batch: bool,
    pub state_ttl_ms: i64,
    pub memory_budget_bytes: i64,
    pub emit_unchanged_with_ttl: bool,
}

/// Deduplication keeping the first row per key (`Deduplicate`, first row).
#[derive(Debug, Clone, PartialEq)]
pub struct KeepFirstParams {
    pub partition_columns: Vec<usize>,
    pub key_timestamp_precisions: Vec<i32>,
    pub rowtime_column: usize,
    pub state_ttl_ms: i64,
    pub memory_budget_bytes: i64,
}

/// Deduplication keeping the last row per key (`Deduplicate`, last row).
#[derive(Debug, Clone, PartialEq)]
pub struct KeepLastParams {
    pub partition_columns: Vec<usize>,
    pub key_timestamp_precisions: Vec<i32>,
    pub rowtime_column: usize,
    pub generate_update_before: bool,
    pub generate_insert: bool,
    pub rowtime_ordered: bool,
    pub keep_first: bool,
    pub mini_batch: bool,
    pub compact_changes: bool,
    pub state_ttl_ms: i64,
    pub memory_budget_bytes: i64,
}

/// One sort key of a Top-N.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub index: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// A streaming Top-N (`Rank` with `ROW_NUMBER`).
#[derive(Debug, Clone, PartialEq)]
pub struct TopNParams {
    pub partition_columns: Vec<usize>,
    pub key_timestamp_precisions: Vec<i32>,
    pub sort: Vec<SortKey>,
    pub offset: i64,
    pub limit: i64,
    pub rank_end_column: i32,
    pub output_rank_number: bool,
    /// The input is a changelog (retracting ranker); else append-only.
    pub retracting: bool,
    pub generate_update_before: bool,
    pub net_diff: bool,
    pub state_ttl_ms: i64,
    pub memory_budget_bytes: i64,
}

/// An event-time interval join (`IntervalJoin`).
#[derive(Debug, Clone, PartialEq)]
pub struct IntervalJoinParams {
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub left_time: usize,
    pub right_time: usize,
    pub lower_ms: i64,
    pub upper_ms: i64,
    /// 0 inner, 1 left outer, 2 right outer, 3 full outer.
    pub join_type: i32,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    /// A non-equi condition, if any.
    pub predicate: Option<EncodedExpr>,
    pub memory_budget_bytes: i64,
}

/// A regular (non-windowed) join over changelogs (`Join`): inner, outer, semi or anti.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdatingJoinParams {
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    /// 0 inner, 1 left outer, 2 right outer, 3 full outer, 4 semi, 5 anti.
    pub join_type: i32,
    /// The data schemas of each side, without the row-kind column.
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    /// A non-equi condition over `[left fields.., right fields..]`, if any.
    pub predicate: Option<EncodedExpr>,
    pub key_timestamp_precisions: Vec<i32>,
    /// Per key: 1 when `=` drops NULL keys, 0 for `IS NOT DISTINCT FROM`; empty for all 1.
    pub filter_nulls: Vec<i32>,
    pub left_unique: bool,
    pub right_unique: bool,
    pub mini_batch: bool,
    pub left_state_ttl_ms: i64,
    pub right_state_ttl_ms: i64,
    pub memory_budget_bytes: i64,
}

/// The windowing TVF on its own (`WindowTableFunction`): each row fanned out to its windows, with
/// `window_start`, `window_end` and `window_time` appended in the time column's timestamp layout.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowTvfParams {
    pub time_column: usize,
    pub shape: WindowShape,
    pub offset_ms: i64,
}

/// Window Top-N and window deduplication (`WindowRank`, `WindowDeduplicate`) over a windowing
/// TVF's output: per window and partition key, the first `limit` rows by `sort`, emitted when the
/// watermark closes the window.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowRankParams {
    pub window_start_column: usize,
    pub window_end_column: usize,
    pub partition_columns: Vec<usize>,
    pub sort: Vec<SortKey>,
    pub limit: i64,
    pub output_rank_number: bool,
    /// Window deduplication keeping the last row: a tie keeps the later row.
    pub keep_last_on_tie: bool,
    pub memory_budget_bytes: i64,
}

/// A window join (`WindowJoin`): two windowing-TVF inputs joined within equal windows.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowJoinParams {
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub left_window_start: usize,
    pub left_window_end: usize,
    pub right_window_start: usize,
    pub right_window_end: usize,
    /// 0 inner, 1 left outer, 2 right outer, 3 full outer, 4 semi, 5 anti.
    pub join_type: i32,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub predicate: Option<EncodedExpr>,
    pub key_timestamp_precisions: Vec<i32>,
    pub memory_budget_bytes: i64,
}

/// The key timestamp precisions to apply: empty means the operator's default (`-1`, not a
/// timestamp, per key column), as when the JNI path passes none.
fn precisions(given: &[i32], keys: usize) -> Vec<i32> {
    if given.is_empty() {
        vec![-1; keys]
    } else {
        given.to_vec()
    }
}

pub(crate) fn calc(p: &CalcParams) -> CalcExpression {
    CalcExpression {
        kinds: p.expr.kinds.clone(),
        payload: p.expr.payload.clone(),
        child_counts: p.expr.child_counts.clone(),
        longs: p.expr.longs.clone(),
        doubles: p.expr.doubles.clone(),
        strings: p.expr.strings.clone(),
        projection_roots: p.projection_roots.clone(),
        condition_root: p.condition_root,
        output_names: p.output_names.clone(),
        compiled: None,
    }
}

pub(crate) fn window_aggregator(
    p: &WindowAggParams,
) -> Result<TumblingAggregator, DataFusionError> {
    let (size, slide, cumulative) = match p.shape {
        WindowShape::Tumble { size_ms } => (size_ms, size_ms, false),
        WindowShape::Hop { size_ms, slide_ms } => (size_ms, slide_ms, false),
        WindowShape::Cumulate {
            max_size_ms,
            step_ms,
        } => (max_size_ms, step_ms, true),
    };
    let mut aggregator = TumblingAggregator::new(
        size,
        slide,
        cumulative,
        p.value_types.clone(),
        p.aggregate_kinds.clone(),
    );
    if p.offset_ms != 0 {
        aggregator.set_assignment_offset(p.offset_ms);
    }
    aggregator.with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn group_aggregator(p: &GroupAggParams) -> Result<GroupAggregator, DataFusionError> {
    let mut aggregator = GroupAggregator::new(
        p.aggregate_kinds.clone(),
        p.value_types.clone(),
        p.value_columns.clone(),
        p.key_columns.clone(),
        p.generate_update_before,
    )
    .with_key_timestamp_precisions(precisions(&p.key_timestamp_precisions, p.key_columns.len()))
    .with_filter_columns(p.filter_columns.clone())
    .with_count_columns(p.count_columns.clone())
    .with_distinct_view_columns(p.distinct_view_columns.clone())
    .with_record_count_column(p.record_count_column)
    .with_state_ttl(p.state_ttl_ms)
    .with_ttl_emission(p.emit_unchanged_with_ttl);
    if p.mini_batch {
        aggregator = aggregator.with_mini_batch();
    }
    aggregator.with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn keep_first(p: &KeepFirstParams) -> Result<KeepFirstDeduplicator, DataFusionError> {
    KeepFirstDeduplicator::new(p.partition_columns.clone(), p.rowtime_column)
        .with_key_timestamp_precisions(precisions(
            &p.key_timestamp_precisions,
            p.partition_columns.len(),
        ))
        .with_state_ttl(p.state_ttl_ms)
        .with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn keep_last(p: &KeepLastParams) -> Result<KeepLastDeduplicator, DataFusionError> {
    KeepLastDeduplicator::new(
        p.partition_columns.clone(),
        p.rowtime_column,
        p.generate_update_before,
        p.rowtime_ordered,
        p.keep_first,
    )
    .with_generate_insert(p.generate_insert)
    .with_mini_batch(p.mini_batch)
    .with_compact_changes(p.compact_changes)
    .with_key_timestamp_precisions(precisions(
        &p.key_timestamp_precisions,
        p.partition_columns.len(),
    ))
    .with_state_ttl(p.state_ttl_ms)
    .with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn top_n(p: &TopNParams) -> Result<TopNHandle, DataFusionError> {
    let sort: Vec<SortColumn> = p
        .sort
        .iter()
        .map(|s| SortColumn {
            index: s.index,
            ascending: s.ascending,
            nulls_first: s.nulls_first,
        })
        .collect();
    let handle = if p.retracting {
        TopNHandle::Retract(
            RetractableTopNRanker::new(
                p.partition_columns.clone(),
                sort,
                p.offset,
                p.limit,
                p.output_rank_number,
            )
            .with_key_timestamp_precisions(precisions(
                &p.key_timestamp_precisions,
                p.partition_columns.len(),
            ))
            .with_rank_end_column(p.rank_end_column)
            .with_generate_update_before(p.generate_update_before)
            .with_net_diff(p.net_diff)
            .with_state_ttl(p.state_ttl_ms),
        )
    } else {
        // The append-only ranker is the no-OFFSET path (offset always 0).
        TopNHandle::Append(
            TopNRanker::new(
                p.partition_columns.clone(),
                sort,
                p.limit,
                p.output_rank_number,
                p.net_diff,
            )
            .with_key_timestamp_precisions(precisions(
                &p.key_timestamp_precisions,
                p.partition_columns.len(),
            ))
            .with_rank_end_column(p.rank_end_column)
            .with_state_ttl(p.state_ttl_ms),
        )
    };
    handle.with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn join_predicate(e: &EncodedExpr) -> Option<JoinPredicate> {
    if e.kinds.is_empty() {
        return None;
    }
    Some(JoinPredicate {
        kinds: e.kinds.clone(),
        payload: e.payload.clone(),
        child_counts: e.child_counts.clone(),
        longs: e.longs.clone(),
        doubles: e.doubles.clone(),
        strings: e.strings.clone(),
        compiled: None,
    })
}

pub(crate) fn interval_joiner(p: &IntervalJoinParams) -> Result<IntervalJoiner, DataFusionError> {
    IntervalJoiner::new(
        p.left_keys.clone(),
        p.right_keys.clone(),
        p.left_time,
        p.right_time,
        p.lower_ms,
        p.upper_ms,
        p.predicate.as_ref().and_then(join_predicate),
        JoinKind::from_code(p.join_type),
        p.left_schema.clone(),
        p.right_schema.clone(),
    )
    .with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn updating_joiner(
    p: &UpdatingJoinParams,
) -> Result<crate::updating_join::UpdatingJoiner, DataFusionError> {
    let keys = p.left_keys.len();
    let filter_nulls = if p.filter_nulls.is_empty() {
        vec![1; keys]
    } else {
        p.filter_nulls.clone()
    };
    crate::updating_join::UpdatingJoiner::new(
        p.left_keys.clone(),
        p.right_keys.clone(),
        JoinKind::from_code(p.join_type),
        p.left_schema.clone(),
        p.right_schema.clone(),
        p.predicate.as_ref().and_then(join_predicate),
    )
    .with_key_timestamp_precisions(precisions(&p.key_timestamp_precisions, keys))
    .with_filter_nulls(filter_nulls)
    .with_unique_join_keys(p.left_unique, p.right_unique)
    .with_mini_batch(p.mini_batch)
    .with_state_ttl(p.left_state_ttl_ms, p.right_state_ttl_ms)
    .with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn window_ranker(
    p: &WindowRankParams,
) -> Result<crate::topn::WindowRanker, DataFusionError> {
    let sort = p
        .sort
        .iter()
        .map(|s| SortColumn {
            index: s.index,
            ascending: s.ascending,
            nulls_first: s.nulls_first,
        })
        .collect();
    let mut ranker = crate::topn::WindowRanker::new(
        p.window_start_column,
        p.window_end_column,
        p.partition_columns.clone(),
        sort,
        p.limit,
        p.output_rank_number,
    );
    if p.keep_last_on_tie {
        ranker.set_keep_last_on_tie(true);
    }
    ranker.with_memory_budget(p.memory_budget_bytes)
}

pub(crate) fn window_joiner(
    p: &WindowJoinParams,
) -> Result<crate::window_join::WindowJoiner, DataFusionError> {
    crate::window_join::WindowJoiner::new(
        p.left_keys.clone(),
        p.right_keys.clone(),
        p.left_window_start,
        p.left_window_end,
        p.right_window_start,
        p.right_window_end,
        p.predicate.as_ref().and_then(join_predicate),
        JoinKind::from_code(p.join_type),
        p.left_schema.clone(),
        p.right_schema.clone(),
    )
    .with_key_timestamp_precisions(precisions(&p.key_timestamp_precisions, p.left_keys.len()))
    .with_memory_budget(p.memory_budget_bytes)
}

/// The stateless windowing TVF.
struct WindowTvf {
    params: WindowTvfParams,
}

impl NativeOperator for WindowTvf {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        let (size, slide, cumulative) = match self.params.shape {
            WindowShape::Tumble { size_ms } => (size_ms, size_ms, false),
            WindowShape::Hop { size_ms, slide_ms } => (size_ms, slide_ms, false),
            WindowShape::Cumulate {
                max_size_ms,
                step_ms,
            } => (max_size_ms, step_ms, true),
        };
        let _ = now;
        let window_type = batch
            .schema()
            .field(self.params.time_column)
            .data_type()
            .clone();
        crate::window_agg::assign_windows(
            &batch,
            self.params.time_column,
            size,
            slide,
            cumulative,
            None,
            &window_type,
            self.params.offset_ms,
        )
        .map(some)
    }
    fn watermark(&mut self, _wm: i64, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
    fn flush(&mut self, _end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
}

impl NativeOperator for crate::topn::WindowRanker {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        crate::topn::WindowRanker::push(self, &batch)?;
        Ok(None)
    }
    fn watermark(
        &mut self,
        watermark: i64,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        crate::topn::WindowRanker::flush(self, watermark).map(some)
    }
    fn flush(&mut self, end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        if end {
            crate::topn::WindowRanker::flush(self, i64::MAX).map(some)
        } else {
            Ok(None)
        }
    }
}

impl NativeOperator for crate::window_join::WindowJoiner {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        match input {
            0 => self.push_left(batch)?,
            1 => self.push_right(batch)?,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "a window join has two inputs, got a batch on input {other}"
                )));
            }
        }
        Ok(None)
    }
    fn watermark(
        &mut self,
        watermark: i64,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        crate::window_join::WindowJoiner::flush(self, watermark).map(some)
    }
    fn flush(&mut self, end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        if end {
            crate::window_join::WindowJoiner::flush(self, i64::MAX).map(some)
        } else {
            Ok(None)
        }
    }
}

/// The parameters of any native operator.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorParams {
    Calc(CalcParams),
    WindowAgg(WindowAggParams),
    GroupAgg(GroupAggParams),
    KeepFirst(KeepFirstParams),
    KeepLast(KeepLastParams),
    TopN(TopNParams),
    IntervalJoin(IntervalJoinParams),
    UpdatingJoin(UpdatingJoinParams),
    WindowTvf(WindowTvfParams),
    WindowRank(WindowRankParams),
    WindowJoin(WindowJoinParams),
}

/// What a runtime drives. Every call returns the rows it produces (`None` for none).
pub trait NativeOperator: Send {
    /// Feeds a batch on input `input` (0; 1 is the right side of a join).
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now_ms: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError>;
    /// Advances event time to `watermark`.
    fn watermark(
        &mut self,
        watermark: i64,
        now_ms: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError>;
    /// Ends a mini-batch, or (with `end_of_input`) the input: buffered rows are emitted.
    fn flush(
        &mut self,
        end_of_input: bool,
        now_ms: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError>;
}

fn some(batch: RecordBatch) -> Option<RecordBatch> {
    (batch.num_rows() > 0).then_some(batch)
}

fn one_input(input: usize) -> Result<(), DataFusionError> {
    if input == 0 {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "operator has one input, got a batch on input {input}"
        )))
    }
}

struct Calc {
    expr: CalcExpression,
    watermark: i64,
}

impl NativeOperator for Calc {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        let expr = &mut self.expr;
        let out =
            crate::flink_functions::clock::with_watermark(self.watermark, || expr.evaluate(batch));
        Ok(some(out))
    }
    fn watermark(
        &mut self,
        watermark: i64,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        self.watermark = watermark;
        Ok(None)
    }
    fn flush(&mut self, _end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
}

impl NativeOperator for TumblingAggregator {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        self.update(&batch)?;
        Ok(None)
    }
    fn watermark(
        &mut self,
        watermark: i64,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        self.flush(watermark).map(some)
    }
    fn flush(&mut self, end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        // At the end of input Flink advances to MAX_WATERMARK, which fires every window.
        if end {
            TumblingAggregator::flush(self, i64::MAX).map(some)
        } else {
            Ok(None)
        }
    }
}

impl NativeOperator for GroupAggregator {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        self.update(&batch, now).map(some)
    }
    fn watermark(&mut self, _wm: i64, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
    fn flush(&mut self, _end: bool, now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        self.flush_mini_batch_at(now).map(some)
    }
}

impl NativeOperator for KeepFirstDeduplicator {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        KeepFirstDeduplicator::push(self, &batch, now)?;
        Ok(None)
    }
    fn watermark(
        &mut self,
        watermark: i64,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        KeepFirstDeduplicator::flush(self, watermark, now).map(some)
    }
    fn flush(&mut self, end: bool, now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        if end {
            KeepFirstDeduplicator::flush(self, i64::MAX, now).map(some)
        } else {
            Ok(None)
        }
    }
}

impl NativeOperator for KeepLastDeduplicator {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        KeepLastDeduplicator::push(self, &batch, now).map(some)
    }
    fn watermark(&mut self, _wm: i64, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
    fn flush(&mut self, _end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        self.flush_mini_batch().map(some)
    }
}

impl NativeOperator for TopNHandle {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        one_input(input)?;
        TopNHandle::push(self, &batch, now).map(some)
    }
    fn watermark(&mut self, _wm: i64, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
    fn flush(&mut self, _end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(some(TopNHandle::flush(self)))
    }
}

impl NativeOperator for IntervalJoiner {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        match input {
            0 => self.push_left(batch, None).map(some),
            1 => self.push_right(batch, None).map(some),
            other => Err(DataFusionError::Execution(format!(
                "an interval join has two inputs, got a batch on input {other}"
            ))),
        }
    }
    fn watermark(
        &mut self,
        watermark: i64,
        _now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        self.advance(watermark).map(some)
    }
    fn flush(&mut self, end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        if end {
            self.advance(i64::MAX).map(some)
        } else {
            Ok(None)
        }
    }
}

impl NativeOperator for crate::updating_join::UpdatingJoiner {
    fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
        now: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        match input {
            0 | 1 => {
                crate::updating_join::UpdatingJoiner::push(self, &batch, input == 0, now).map(some)
            }
            other => Err(DataFusionError::Execution(format!(
                "a join has two inputs, got a batch on input {other}"
            ))),
        }
    }
    fn watermark(&mut self, _wm: i64, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        Ok(None)
    }
    fn flush(&mut self, _end: bool, _now: i64) -> Result<Option<RecordBatch>, DataFusionError> {
        self.flush_mini_batch().map(some)
    }
}

/// Builds the operator `params` describes.
pub fn build(params: &OperatorParams) -> Result<Box<dyn NativeOperator>, DataFusionError> {
    Ok(match params {
        OperatorParams::Calc(p) => Box::new(Calc {
            expr: calc(p),
            watermark: i64::MIN,
        }),
        OperatorParams::WindowAgg(p) => Box::new(window_aggregator(p)?),
        OperatorParams::GroupAgg(p) => Box::new(group_aggregator(p)?),
        OperatorParams::KeepFirst(p) => Box::new(keep_first(p)?),
        OperatorParams::KeepLast(p) => Box::new(keep_last(p)?),
        OperatorParams::TopN(p) => Box::new(top_n(p)?),
        OperatorParams::IntervalJoin(p) => Box::new(interval_joiner(p)?),
        OperatorParams::UpdatingJoin(p) => Box::new(updating_joiner(p)?),
        OperatorParams::WindowTvf(p) => Box::new(WindowTvf { params: p.clone() }),
        OperatorParams::WindowRank(p) => Box::new(window_ranker(p)?),
        OperatorParams::WindowJoin(p) => Box::new(window_joiner(p)?),
    })
}
