// Without the `jni` feature the JNI entry points, the only callers of most operators today, are
// compiled out, so the operator code reads as dead until the Rust operator driver (FC1 Task 2)
// becomes its caller.
#![cfg_attr(not(feature = "jni"), allow(dead_code, unused_imports))]
pub(crate) use datafusion::catalog::memory::MemorySourceConfig;
pub(crate) use datafusion::common::{DFSchema, DataFusionError, JoinSide, JoinType, NullEquality};
pub(crate) use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
pub(crate) use datafusion::execution::runtime_env::RuntimeEnvBuilder;
pub(crate) use datafusion::execution::TaskContext;
pub(crate) use datafusion::functions_aggregate::count::count_udaf;
pub(crate) use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
pub(crate) use datafusion::functions_aggregate::sum::sum_udaf;
pub(crate) use datafusion::logical_expr::execution_props::ExecutionProps;
pub(crate) use datafusion::logical_expr::{Accumulator, AggregateUDF, Operator};
pub(crate) use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
pub(crate) use datafusion::physical_expr::aggregate::{
    AggregateExprBuilder, AggregateFunctionExpr,
};
pub(crate) use datafusion::physical_expr::expressions::{binary, col, lit, Column};
pub(crate) use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
pub(crate) use datafusion::physical_plan::collect;
pub(crate) use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
pub(crate) use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
pub(crate) use datafusion::prelude::{col as logical_col, lit as logical_lit, SessionContext};
pub(crate) use datafusion::scalar::ScalarValue;
pub(crate) use futures::StreamExt;
#[allow(unused_imports)]
use streamfusion_bridge::prelude::*;
#[allow(unused_imports)]
use streamfusion_bridge::{flink_text, json_string, *};
pub(crate) use tokio::runtime::Runtime;

streamfusion_bridge::link_allocator!();
mod aggregates;
mod append_buffer;
mod bridge;
mod bucket_route;
mod calc;
mod changelog_filter;
mod dedup;
mod exchange;
mod expr;
mod file_partition;
mod first_n;
mod flatten;
mod flink_float;
mod flink_functions;
mod flink_key;
mod group_agg;
mod interval_join;
mod ipc;
mod join_common;
mod keyed_upsert;
mod keys;
#[cfg(feature = "jni")]
mod logging;
mod memory;
mod mini_batch;
mod normalizer;
mod over_agg;
mod rowtime;
mod session_agg;
mod sink_materializer;
mod sorter;
mod state;
mod temporal_join;
mod topn;
mod updating_join;
mod window_agg;
mod window_calendar;

/// A host (JVM) object the engine calls back into. Keyed on the engine's own `jni` feature, not
/// the bridge's: when another crate in the same build turns the bridge's `jni` on, the engine
/// without `jni` must still see an uninhabited type, so its upcall paths stay unreachable.
#[cfg(feature = "jni")]
pub(crate) type HostObject = streamfusion_bridge::bridge::HostObject;
#[cfg(not(feature = "jni"))]
pub(crate) type HostObject = std::convert::Infallible;
mod window_join;

pub(crate) use bridge::*;
#[allow(unused_imports)]
pub(crate) use {
    aggregates::*, calc::*, dedup::*, exchange::*, expr::*, first_n::*, flatten::*, flink_key::*,
    group_agg::*, interval_join::*, ipc::*, join_common::*, keyed_upsert::*, keys::*, memory::*,
    mini_batch::*, normalizer::*, over_agg::*, rowtime::*, session_agg::*, sorter::*, state::*,
    temporal_join::*, topn::*, updating_join::*, window_agg::*, window_join::*,
};

/// The engine's build version (the `native/` workspace's package version).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod bench;
#[cfg(feature = "semantics")]
pub mod semantics;
#[cfg(test)]
mod tests;
