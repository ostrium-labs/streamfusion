//! `OpSpec`: the engine's physical IR, one variant per native operator (FC1 Task 2; design 82
//! §5.3).
//!
//! StreamFusion's Java plugin matches Flink ExecNodes and constructs each native operator through
//! a JNI call with its parameters. An `OpSpec` carries exactly those parameters, in a serialisable
//! form, so the planner (Task 10), the ExecNode-JSON reader and tests can describe operators
//! without the engine; `loams-stream-runtime` maps a spec onto the engine's constructors. This crate
//! does not depend on the engine.
//!
//! `memory_budget_bytes` follows the engine: -1 for no budget, a positive size for a private pool,
//! below -1 for a share of the task's off-heap pool.
//!
//! Codes are the engine's: aggregate kinds and value types as `aggregates.rs` defines them,
//! expression node kinds as the engine's encoded-expression decoder (`expr.rs`) reads them.

use arrow_schema::Field;
use serde::{Deserialize, Serialize};

/// The version of the serialised form; bumped on any change that is not backward compatible.
pub const OPSPEC_VERSION: u32 = 1;

/// An expression tree in the engine's prefix encoding (see the engine's `ops::EncodedExpr`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EncodedExpr {
    pub kinds: Vec<i64>,
    pub payload: Vec<i64>,
    pub child_counts: Vec<i64>,
    pub longs: Vec<i64>,
    pub doubles: Vec<f64>,
    pub strings: Vec<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalcSpec {
    pub expr: EncodedExpr,
    pub projection_roots: Vec<usize>,
    /// The filter's root node, or -1 for none.
    pub condition_root: i64,
    pub output_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowShape {
    Tumble { size_ms: i64 },
    Hop { size_ms: i64, slide_ms: i64 },
    Cumulate { max_size_ms: i64, step_ms: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowAggSpec {
    pub shape: WindowShape,
    pub value_types: Vec<i64>,
    pub aggregate_kinds: Vec<i64>,
    /// The windows' offset from the epoch, in milliseconds.
    #[serde(default)]
    pub offset_ms: i64,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupAggSpec {
    pub aggregate_kinds: Vec<i64>,
    pub value_types: Vec<i64>,
    pub value_columns: Vec<i64>,
    pub key_columns: Vec<usize>,
    #[serde(default)]
    pub key_timestamp_precisions: Vec<i32>,
    #[serde(default)]
    pub filter_columns: Vec<i64>,
    #[serde(default)]
    pub count_columns: Vec<i64>,
    #[serde(default)]
    pub distinct_view_columns: Vec<i64>,
    #[serde(default = "minus_one")]
    pub record_count_column: i64,
    pub generate_update_before: bool,
    #[serde(default)]
    pub mini_batch: bool,
    #[serde(default)]
    pub state_ttl_ms: i64,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
    #[serde(default)]
    pub emit_unchanged_with_ttl: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepFirstSpec {
    pub partition_columns: Vec<usize>,
    #[serde(default)]
    pub key_timestamp_precisions: Vec<i32>,
    pub rowtime_column: usize,
    #[serde(default)]
    pub state_ttl_ms: i64,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepLastSpec {
    pub partition_columns: Vec<usize>,
    #[serde(default)]
    pub key_timestamp_precisions: Vec<i32>,
    pub rowtime_column: usize,
    pub generate_update_before: bool,
    pub generate_insert: bool,
    pub rowtime_ordered: bool,
    pub keep_first: bool,
    #[serde(default)]
    pub mini_batch: bool,
    #[serde(default)]
    pub compact_changes: bool,
    #[serde(default)]
    pub state_ttl_ms: i64,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    pub index: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopNSpec {
    pub partition_columns: Vec<usize>,
    #[serde(default)]
    pub key_timestamp_precisions: Vec<i32>,
    pub sort: Vec<SortKey>,
    #[serde(default)]
    pub offset: i64,
    pub limit: i64,
    #[serde(default = "minus_one_i32")]
    pub rank_end_column: i32,
    pub output_rank_number: bool,
    pub retracting: bool,
    #[serde(default)]
    pub generate_update_before: bool,
    #[serde(default)]
    pub net_diff: bool,
    #[serde(default)]
    pub state_ttl_ms: i64,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntervalJoinSpec {
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub left_time: usize,
    pub right_time: usize,
    pub lower_ms: i64,
    pub upper_ms: i64,
    /// 0 inner, 1 left outer, 2 right outer, 3 full outer.
    pub join_type: i32,
    pub left_fields: Vec<Field>,
    pub right_fields: Vec<Field>,
    #[serde(default)]
    pub predicate: Option<EncodedExpr>,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

/// A regular join over changelogs (`Join`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdatingJoinSpec {
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    /// 0 inner, 1 left outer, 2 right outer, 3 full outer, 4 semi, 5 anti.
    pub join_type: i32,
    pub left_fields: Vec<Field>,
    pub right_fields: Vec<Field>,
    #[serde(default)]
    pub predicate: Option<EncodedExpr>,
    #[serde(default)]
    pub key_timestamp_precisions: Vec<i32>,
    #[serde(default)]
    pub filter_nulls: Vec<i32>,
    #[serde(default)]
    pub left_unique: bool,
    #[serde(default)]
    pub right_unique: bool,
    #[serde(default)]
    pub mini_batch: bool,
    #[serde(default)]
    pub left_state_ttl_ms: i64,
    #[serde(default)]
    pub right_state_ttl_ms: i64,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

/// The windowing TVF on its own (`WindowTableFunction`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowTvfSpec {
    pub time_column: usize,
    pub shape: WindowShape,
    #[serde(default)]
    pub offset_ms: i64,
}

/// Window Top-N and window deduplication (`WindowRank`, `WindowDeduplicate`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowRankSpec {
    pub window_start_column: usize,
    pub window_end_column: usize,
    pub partition_columns: Vec<usize>,
    pub sort: Vec<SortKey>,
    pub limit: i64,
    pub output_rank_number: bool,
    #[serde(default)]
    pub keep_last_on_tie: bool,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

/// A window join (`WindowJoin`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowJoinSpec {
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub left_window_start: usize,
    pub left_window_end: usize,
    pub right_window_start: usize,
    pub right_window_end: usize,
    /// 0 inner, 1 left outer, 2 right outer, 3 full outer, 4 semi, 5 anti.
    pub join_type: i32,
    pub left_fields: Vec<Field>,
    pub right_fields: Vec<Field>,
    #[serde(default)]
    pub predicate: Option<EncodedExpr>,
    #[serde(default)]
    pub key_timestamp_precisions: Vec<i32>,
    #[serde(default = "unbounded")]
    pub memory_budget_bytes: i64,
}

/// One native operator and its parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum OpSpec {
    Calc(CalcSpec),
    WindowAgg(WindowAggSpec),
    GroupAgg(GroupAggSpec),
    KeepFirst(KeepFirstSpec),
    KeepLast(KeepLastSpec),
    TopN(TopNSpec),
    IntervalJoin(IntervalJoinSpec),
    UpdatingJoin(UpdatingJoinSpec),
    WindowTvf(WindowTvfSpec),
    WindowRank(WindowRankSpec),
    WindowJoin(WindowJoinSpec),
}

impl OpSpec {
    /// The number of inputs the operator takes.
    pub fn inputs(&self) -> usize {
        match self {
            OpSpec::IntervalJoin(_) | OpSpec::UpdatingJoin(_) | OpSpec::WindowJoin(_) => 2,
            _ => 1,
        }
    }
}

/// `memory_budget_bytes` of -1: no memory budget (the engine's convention).
fn unbounded() -> i64 {
    -1
}

fn minus_one() -> i64 {
    -1
}

fn minus_one_i32() -> i32 {
    -1
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn every_variant() -> Vec<OpSpec> {
        vec![
            OpSpec::Calc(CalcSpec {
                expr: EncodedExpr {
                    kinds: vec![1, 0, 0],
                    payload: vec![0, 0, 1],
                    child_counts: vec![2, 0, 0],
                    longs: vec![],
                    doubles: vec![0.5],
                    strings: vec![Some("x".into()), None],
                },
                projection_roots: vec![0],
                condition_root: -1,
                output_names: vec!["a".into()],
            }),
            OpSpec::WindowAgg(WindowAggSpec {
                shape: WindowShape::Hop {
                    size_ms: 10_000,
                    slide_ms: 2_000,
                },
                value_types: vec![0],
                aggregate_kinds: vec![3],
                offset_ms: 0,
                memory_budget_bytes: -1,
            }),
            OpSpec::GroupAgg(GroupAggSpec {
                aggregate_kinds: vec![0],
                value_types: vec![0],
                value_columns: vec![1],
                key_columns: vec![0],
                key_timestamp_precisions: vec![-1],
                filter_columns: vec![-1],
                count_columns: vec![],
                distinct_view_columns: vec![],
                record_count_column: -1,
                generate_update_before: true,
                mini_batch: false,
                state_ttl_ms: 0,
                memory_budget_bytes: -1,
                emit_unchanged_with_ttl: false,
            }),
            OpSpec::KeepFirst(KeepFirstSpec {
                partition_columns: vec![0],
                key_timestamp_precisions: vec![],
                rowtime_column: 1,
                state_ttl_ms: 0,
                memory_budget_bytes: -1,
            }),
            OpSpec::KeepLast(KeepLastSpec {
                partition_columns: vec![0],
                key_timestamp_precisions: vec![],
                rowtime_column: 1,
                generate_update_before: true,
                generate_insert: true,
                rowtime_ordered: false,
                keep_first: false,
                mini_batch: false,
                compact_changes: false,
                state_ttl_ms: 0,
                memory_budget_bytes: -1,
            }),
            OpSpec::TopN(TopNSpec {
                partition_columns: vec![0],
                key_timestamp_precisions: vec![],
                sort: vec![SortKey {
                    index: 1,
                    ascending: false,
                    nulls_first: false,
                }],
                offset: 0,
                limit: 3,
                rank_end_column: -1,
                output_rank_number: true,
                retracting: false,
                generate_update_before: false,
                net_diff: false,
                state_ttl_ms: 0,
                memory_budget_bytes: -1,
            }),
            OpSpec::IntervalJoin(IntervalJoinSpec {
                left_keys: vec![0],
                right_keys: vec![0],
                left_time: 1,
                right_time: 1,
                lower_ms: -5_000,
                upper_ms: 5_000,
                join_type: 0,
                left_fields: vec![Field::new("k", DataType::Int64, false)],
                right_fields: vec![Field::new("k", DataType::Int64, false)],
                predicate: None,
                memory_budget_bytes: -1,
            }),
            OpSpec::UpdatingJoin(UpdatingJoinSpec {
                left_keys: vec![0],
                right_keys: vec![1],
                join_type: 1,
                left_fields: vec![Field::new("k", DataType::Int64, false)],
                right_fields: vec![Field::new("v", DataType::Utf8, true)],
                predicate: None,
                key_timestamp_precisions: vec![-1],
                filter_nulls: vec![1],
                left_unique: false,
                right_unique: false,
                mini_batch: false,
                left_state_ttl_ms: 0,
                right_state_ttl_ms: 0,
                memory_budget_bytes: -1,
            }),
            OpSpec::WindowTvf(WindowTvfSpec {
                time_column: 2,
                shape: WindowShape::Tumble { size_ms: 1000 },
                offset_ms: 0,
            }),
            OpSpec::WindowRank(WindowRankSpec {
                window_start_column: 3,
                window_end_column: 4,
                partition_columns: vec![0],
                sort: vec![SortKey {
                    index: 1,
                    ascending: true,
                    nulls_first: true,
                }],
                limit: 3,
                output_rank_number: false,
                keep_last_on_tie: false,
                memory_budget_bytes: -1,
            }),
            OpSpec::WindowJoin(WindowJoinSpec {
                left_keys: vec![0],
                right_keys: vec![0],
                left_window_start: 1,
                left_window_end: 2,
                right_window_start: 1,
                right_window_end: 2,
                join_type: 0,
                left_fields: vec![Field::new("k", DataType::Int64, false)],
                right_fields: vec![Field::new("k", DataType::Int64, false)],
                predicate: None,
                key_timestamp_precisions: vec![],
                memory_budget_bytes: -1,
            }),
        ]
    }

    #[test]
    fn opspec_round_trips() {
        for spec in every_variant() {
            let json = serde_json::to_string(&spec).unwrap();
            let back: OpSpec = serde_json::from_str(&json).unwrap();
            assert_eq!(back, spec, "{json}");
        }
    }

    #[test]
    fn defaults_fill_optional_fields() {
        let spec: OpSpec = serde_json::from_str(
            r#"{"op":"keep_first","partition_columns":[0],"rowtime_column":1}"#,
        )
        .unwrap();
        let OpSpec::KeepFirst(k) = spec else { panic!() };
        assert_eq!(k.memory_budget_bytes, -1);
        assert_eq!(k.state_ttl_ms, 0);
    }
}
