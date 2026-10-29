//! The physical plan: a DAG of operators the runtime executes (FC1 Task 10).
//!
//! Native operators carry their [`OpSpec`]; the nodes Flink's Java plugin finishes on the JVM
//! side (sources, the watermark assigner, unions, sinks) are described here for the runtime.

use std::collections::BTreeMap;

use crate::changelog::ChangelogMode;
use crate::expr::Literal;
use crate::logical::Field;
use crate::opspec::OpSpec;

pub type NodeId = usize;

/// How records move from a producer to a consumer's subtasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Distribution {
    /// Same subtask (an operator chain).
    Forward,
    /// By the hash of these fields, with Flink's key groups.
    Hash(Vec<usize>),
    /// Everything to one subtask.
    Single,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    pub node: NodeId,
    pub distribution: Distribution,
}

/// A table read or written by connector.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectorTable {
    /// `catalog.database.table`.
    pub name: String,
    pub options: BTreeMap<String, String>,
    pub fields: Vec<Field>,
    pub primary_key: Option<Vec<usize>>,
}

impl ConnectorTable {
    pub fn connector(&self) -> &str {
        self.options
            .get("connector")
            .map(String::as_str)
            .unwrap_or("")
    }

    pub fn format(&self) -> Option<&str> {
        self.options
            .get("format")
            .or_else(|| self.options.get("value.format"))
            .map(String::as_str)
    }
}

/// Watermarks `rowtime - delay` from the largest rowtime seen (Flink's bounded
/// out-of-orderness, the only watermark expression the native assigner admits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatermarkSpec {
    pub rowtime: usize,
    pub delay_ms: i64,
}

/// A physical operator.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalOp {
    /// `TableSourceScan`.
    Source(ConnectorTable),
    /// `Values`: constant rows.
    Values(Vec<Vec<Literal>>),
    /// `WatermarkAssigner`.
    Watermark(WatermarkSpec),
    /// A native operator (`Calc`, window and group aggregates, Top-N, deduplication, joins).
    Native(Box<OpSpec>),
    /// `Union` (ALL) of its inputs.
    Union,
    /// `Sink`.
    Sink(ConnectorTable),
}

/// One node of the plan.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalNode {
    pub id: NodeId,
    /// Flink's ExecNode name (`Calc`, `GroupAggregate`, ...), as `EXPLAIN` prints it.
    pub name: String,
    /// The node's attributes as `EXPLAIN` prints them, e.g. `select=[a, b]`.
    pub description: String,
    pub op: PhysicalOp,
    pub inputs: Vec<Edge>,
    pub fields: Vec<Field>,
    pub changelog: ChangelogMode,
    /// The fields that identify a row for upserts, when known.
    pub upsert_key: Option<Vec<usize>>,
}

/// A planned job: the nodes in topological order (inputs before consumers).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PhysicalPlan {
    pub nodes: Vec<PhysicalNode>,
}

impl PhysicalPlan {
    pub fn node(&self, id: NodeId) -> &PhysicalNode {
        &self.nodes[id]
    }

    /// The consumers of `id`.
    pub fn consumers(&self, id: NodeId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.inputs.iter().any(|e| e.node == id))
            .map(|n| n.id)
            .collect()
    }

    pub fn sinks(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| matches!(n.op, PhysicalOp::Sink(_)))
            .map(|n| n.id)
            .collect()
    }

    /// The operator names, as the corpus's `operators.txt` lists them (sorted, unique).
    pub fn operator_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.nodes.iter().map(|n| n.name.clone()).collect();
        names.sort();
        names.dedup();
        names
    }
}
