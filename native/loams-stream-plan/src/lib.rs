//! The streaming planner: changelog and time-attribute inference, streaming optimiser rules,
//! lowering to StreamFusion's operator descriptions, and the ExecNode-JSON reader.
//!
//! Part of Loams Stream, the Apache Flink-compatible engine (Loams design 82). **Preview**: only
//! the physical IR ([`OpSpec`]) exists so far; nothing in this crate has run end to end.

pub mod opspec;

pub use opspec::{
    CalcSpec, EncodedExpr, GroupAggSpec, IntervalJoinSpec, KeepFirstSpec, KeepLastSpec,
    OPSPEC_VERSION, OpSpec, SortKey, TopNSpec, WindowAggSpec, WindowShape,
};
