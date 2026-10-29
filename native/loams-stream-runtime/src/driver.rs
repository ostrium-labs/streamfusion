//! The operator driver: builds a native operator from an [`OpSpec`] and drives it the way Flink's
//! operator chain drives it through the JNI plugin, without Flink or JNI (FC1 Task 2).
//!
//! # The contract
//!
//! This is what a native operator expects from whatever runs it, and what the full runtime
//! (Task 11) implements:
//!
//! - **Batches** arrive in the operator's input column layout (see the engine's `ops::*Params`);
//!   a changelog carries Flink's row kind in an `Int8` column `$row_kind$` (0 `+I`, 1 `-U`, 2 `+U`,
//!   3 `-D`). Output batches use the same convention.
//! - **Keys** need no context: the operator hashes its key columns per row, with Flink's key
//!   hashing and key groups (`flink_key`), so keyed state partitions as Flink's does.
//! - **Event time** advances only through [`Driver::watermark`]; watermarks never go back. Rows an
//!   operator releases on a watermark (closed windows, expired join rows) are returned by that
//!   call.
//! - **Processing time** is the `now_ms` the driver passes on every call ([`Driver::set_now`]).
//! - **Mini-batches and the end of input** are [`Driver::flush`]; at the end of input event time
//!   jumps to `i64::MAX`, as Flink's `MAX_WATERMARK` does.
//! - **Checkpoints** (snapshot and restore by key group) are not in this contract yet; Task 13
//!   adds them.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use loams_stream_plan::{EncodedExpr, OpSpec, WindowShape};
use streamfusion::ops::{self, NativeOperator, OperatorParams};

/// A driver failure: a spec the engine refuses, or an operator error.
#[derive(Debug)]
pub enum DriverError {
    /// The operator failed (memory budget, a bad batch).
    Operator(String),
    /// Event time went back.
    WatermarkRegressed { current: i64, new: i64 },
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverError::Operator(m) => f.write_str(m),
            DriverError::WatermarkRegressed { current, new } => {
                write!(f, "watermark went back from {current} to {new}")
            }
        }
    }
}

impl std::error::Error for DriverError {}

fn op_err(e: impl std::fmt::Display) -> DriverError {
    DriverError::Operator(e.to_string())
}

fn expr(e: &EncodedExpr) -> ops::EncodedExpr {
    ops::EncodedExpr {
        kinds: e.kinds.clone(),
        payload: e.payload.clone(),
        child_counts: e.child_counts.clone(),
        longs: e.longs.clone(),
        doubles: e.doubles.clone(),
        strings: e.strings.clone(),
    }
}

/// The engine parameters an [`OpSpec`] describes. Every field maps one to one, so a field added on
/// either side fails to compile here rather than being dropped.
pub fn engine_params(spec: &OpSpec) -> OperatorParams {
    match spec {
        OpSpec::Calc(s) => OperatorParams::Calc(ops::CalcParams {
            expr: expr(&s.expr),
            projection_roots: s.projection_roots.clone(),
            condition_root: s.condition_root,
            output_names: s.output_names.clone(),
        }),
        OpSpec::WindowAgg(s) => OperatorParams::WindowAgg(ops::WindowAggParams {
            shape: match s.shape {
                WindowShape::Tumble { size_ms } => ops::WindowShape::Tumble { size_ms },
                WindowShape::Hop { size_ms, slide_ms } => {
                    ops::WindowShape::Hop { size_ms, slide_ms }
                }
                WindowShape::Cumulate {
                    max_size_ms,
                    step_ms,
                } => ops::WindowShape::Cumulate {
                    max_size_ms,
                    step_ms,
                },
            },
            value_types: s.value_types.clone(),
            aggregate_kinds: s.aggregate_kinds.clone(),
            memory_budget_bytes: s.memory_budget_bytes,
        }),
        OpSpec::GroupAgg(s) => OperatorParams::GroupAgg(ops::GroupAggParams {
            aggregate_kinds: s.aggregate_kinds.clone(),
            value_types: s.value_types.clone(),
            value_columns: s.value_columns.clone(),
            key_columns: s.key_columns.clone(),
            key_timestamp_precisions: s.key_timestamp_precisions.clone(),
            filter_columns: s.filter_columns.clone(),
            count_columns: s.count_columns.clone(),
            distinct_view_columns: s.distinct_view_columns.clone(),
            record_count_column: s.record_count_column,
            generate_update_before: s.generate_update_before,
            mini_batch: s.mini_batch,
            state_ttl_ms: s.state_ttl_ms,
            memory_budget_bytes: s.memory_budget_bytes,
            emit_unchanged_with_ttl: s.emit_unchanged_with_ttl,
        }),
        OpSpec::KeepFirst(s) => OperatorParams::KeepFirst(ops::KeepFirstParams {
            partition_columns: s.partition_columns.clone(),
            key_timestamp_precisions: s.key_timestamp_precisions.clone(),
            rowtime_column: s.rowtime_column,
            state_ttl_ms: s.state_ttl_ms,
            memory_budget_bytes: s.memory_budget_bytes,
        }),
        OpSpec::KeepLast(s) => OperatorParams::KeepLast(ops::KeepLastParams {
            partition_columns: s.partition_columns.clone(),
            key_timestamp_precisions: s.key_timestamp_precisions.clone(),
            rowtime_column: s.rowtime_column,
            generate_update_before: s.generate_update_before,
            generate_insert: s.generate_insert,
            rowtime_ordered: s.rowtime_ordered,
            keep_first: s.keep_first,
            mini_batch: s.mini_batch,
            compact_changes: s.compact_changes,
            state_ttl_ms: s.state_ttl_ms,
            memory_budget_bytes: s.memory_budget_bytes,
        }),
        OpSpec::TopN(s) => OperatorParams::TopN(ops::TopNParams {
            partition_columns: s.partition_columns.clone(),
            key_timestamp_precisions: s.key_timestamp_precisions.clone(),
            sort: s
                .sort
                .iter()
                .map(|k| ops::SortKey {
                    index: k.index,
                    ascending: k.ascending,
                    nulls_first: k.nulls_first,
                })
                .collect(),
            offset: s.offset,
            limit: s.limit,
            rank_end_column: s.rank_end_column,
            output_rank_number: s.output_rank_number,
            retracting: s.retracting,
            generate_update_before: s.generate_update_before,
            net_diff: s.net_diff,
            state_ttl_ms: s.state_ttl_ms,
            memory_budget_bytes: s.memory_budget_bytes,
        }),
        OpSpec::IntervalJoin(s) => OperatorParams::IntervalJoin(ops::IntervalJoinParams {
            left_keys: s.left_keys.clone(),
            right_keys: s.right_keys.clone(),
            left_time: s.left_time,
            right_time: s.right_time,
            lower_ms: s.lower_ms,
            upper_ms: s.upper_ms,
            join_type: s.join_type,
            left_schema: Arc::new(Schema::new(s.left_fields.clone())),
            right_schema: Arc::new(Schema::new(s.right_fields.clone())),
            predicate: s.predicate.as_ref().map(expr),
            memory_budget_bytes: s.memory_budget_bytes,
        }),
    }
}

/// One native operator, driven batch by batch.
pub struct Driver {
    operator: Box<dyn NativeOperator>,
    inputs: usize,
    watermark: i64,
    now_ms: i64,
}

impl Driver {
    /// Builds the operator `spec` describes.
    pub fn new(spec: &OpSpec) -> Result<Self, DriverError> {
        Ok(Driver {
            operator: ops::build(&engine_params(spec)).map_err(op_err)?,
            inputs: spec.inputs(),
            watermark: i64::MIN,
            now_ms: 0,
        })
    }

    /// Sets the processing-time clock.
    pub fn set_now(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
    }

    /// The current watermark.
    pub fn current_watermark(&self) -> i64 {
        self.watermark
    }

    /// Feeds `batch` on `input`; returns the rows it produced.
    pub fn push(
        &mut self,
        input: usize,
        batch: RecordBatch,
    ) -> Result<Vec<RecordBatch>, DriverError> {
        if input >= self.inputs {
            return Err(DriverError::Operator(format!(
                "the operator has {} input(s), got a batch on input {input}",
                self.inputs
            )));
        }
        Ok(self
            .operator
            .push(input, batch, self.now_ms)
            .map_err(op_err)?
            .into_iter()
            .collect())
    }

    /// Advances event time; returns the rows the watermark releases.
    pub fn watermark(&mut self, watermark: i64) -> Result<Vec<RecordBatch>, DriverError> {
        if watermark < self.watermark {
            return Err(DriverError::WatermarkRegressed {
                current: self.watermark,
                new: watermark,
            });
        }
        self.watermark = watermark;
        Ok(self
            .operator
            .watermark(watermark, self.now_ms)
            .map_err(op_err)?
            .into_iter()
            .collect())
    }

    /// Ends a mini-batch; returns buffered rows.
    pub fn flush(&mut self) -> Result<Vec<RecordBatch>, DriverError> {
        Ok(self
            .operator
            .flush(false, self.now_ms)
            .map_err(op_err)?
            .into_iter()
            .collect())
    }

    /// Ends the input: event time goes to the end and every buffered row is emitted.
    pub fn finish(&mut self) -> Result<Vec<RecordBatch>, DriverError> {
        self.watermark = i64::MAX;
        Ok(self
            .operator
            .flush(true, self.now_ms)
            .map_err(op_err)?
            .into_iter()
            .collect())
    }
}
