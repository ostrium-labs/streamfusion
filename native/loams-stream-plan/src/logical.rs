//! The logical plan: relational operators over typed fields, the planner's counterpart of
//! Calcite's `RelNode` tree after Flink's logical rewrites (FC1 Task 10).

use std::collections::BTreeMap;

use loams_stream_sql::binder::ObjectIdentifier;
use loams_stream_sql::types::LogicalType;

use crate::expr::ScalarExpr;

/// A named, typed field of a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub ty: LogicalType,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: LogicalType) -> Self {
        Field {
            name: name.into(),
            ty,
        }
    }
}

/// A catalog table read by a query or written by an `INSERT`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub id: ObjectIdentifier,
    /// The `WITH` options, with `OPTIONS` hints applied.
    pub options: BTreeMap<String, String>,
    /// The physical columns, in declaration order.
    pub physical: Vec<Field>,
    /// The primary key, as indexes into `physical`.
    pub primary_key: Option<Vec<usize>>,
}

/// A window table-valued function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    Tumble {
        size_ms: i64,
        offset_ms: i64,
    },
    Hop {
        size_ms: i64,
        slide_ms: i64,
        offset_ms: i64,
    },
    Cumulate {
        max_size_ms: i64,
        step_ms: i64,
        offset_ms: i64,
    },
    Session {
        gap_ms: i64,
    },
}

/// The join kinds of a binary join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// An aggregate function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggFunc {
    /// `COUNT(*)`.
    CountStar,
    Count,
    Sum,
    /// `SUM` that is 0 rather than NULL over no rows (Calcite's `$SUM0`, from `AVG`'s split).
    Sum0,
    Min,
    Max,
    Avg,
    FirstValue,
    LastValue,
    ListAgg,
    /// Any other aggregate, by its upper-case name.
    Other(String),
}

/// One aggregate call of an [`Rel::Aggregate`].
#[derive(Debug, Clone, PartialEq)]
pub struct AggCall {
    pub func: AggFunc,
    /// Input field indexes of the arguments.
    pub args: Vec<usize>,
    pub distinct: bool,
    /// The input field holding the `FILTER (WHERE ...)` condition.
    pub filter: Option<usize>,
    pub ty: LogicalType,
    pub name: String,
}

/// A sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub index: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// A ranking function of a Top-N.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankFunc {
    RowNumber,
    Rank,
    DenseRank,
}

/// A logical relation.
#[derive(Debug, Clone, PartialEq)]
pub enum Rel {
    /// A catalog table's physical columns.
    Scan { table: TableRef, fields: Vec<Field> },
    /// `VALUES` rows of literals.
    Values {
        rows: Vec<Vec<ScalarExpr>>,
        fields: Vec<Field>,
    },
    Project {
        input: Box<Rel>,
        exprs: Vec<ScalarExpr>,
        fields: Vec<Field>,
    },
    Filter {
        input: Box<Rel>,
        condition: ScalarExpr,
    },
    /// Watermarks from a table's `WATERMARK FOR rowtime AS expr`.
    Watermark {
        input: Box<Rel>,
        rowtime: usize,
        expr: ScalarExpr,
    },
    /// A window TVF: the input's fields plus `window_start`, `window_end` and `window_time`.
    WindowTvf {
        input: Box<Rel>,
        time: usize,
        window: WindowKind,
        fields: Vec<Field>,
    },
    /// `GROUP BY` (no calls: `DISTINCT`). Output: the group fields, then one per call.
    Aggregate {
        input: Box<Rel>,
        group: Vec<usize>,
        calls: Vec<AggCall>,
        fields: Vec<Field>,
    },
    Join {
        left: Box<Rel>,
        right: Box<Rel>,
        kind: JoinKind,
        condition: ScalarExpr,
        fields: Vec<Field>,
    },
    /// `UNION ALL`.
    Union {
        inputs: Vec<Rel>,
        fields: Vec<Field>,
    },
    /// A Top-N: the input's fields, plus the rank when `output_rank` is set.
    Rank {
        input: Box<Rel>,
        func: RankFunc,
        partition: Vec<usize>,
        order: Vec<SortKey>,
        /// 1-based, inclusive.
        rank_start: i64,
        rank_end: i64,
        output_rank: bool,
        fields: Vec<Field>,
    },
    /// `ORDER BY ... [LIMIT ...]` or `LIMIT` alone (empty keys).
    Sort {
        input: Box<Rel>,
        keys: Vec<SortKey>,
        offset: i64,
        fetch: Option<i64>,
    },
}

impl Rel {
    pub fn fields(&self) -> &[Field] {
        match self {
            Rel::Scan { fields, .. }
            | Rel::Values { fields, .. }
            | Rel::Project { fields, .. }
            | Rel::WindowTvf { fields, .. }
            | Rel::Aggregate { fields, .. }
            | Rel::Join { fields, .. }
            | Rel::Union { fields, .. }
            | Rel::Rank { fields, .. } => fields,
            Rel::Filter { input, .. } | Rel::Watermark { input, .. } | Rel::Sort { input, .. } => {
                input.fields()
            }
        }
    }

    /// A projection of `exprs` named `names` over `self`; an identity projection is dropped.
    pub fn project(self, exprs: Vec<ScalarExpr>, names: Vec<String>) -> Rel {
        let identity = exprs.len() == self.fields().len()
            && exprs
                .iter()
                .enumerate()
                .all(|(i, e)| e.as_input() == Some(i))
            && names.iter().zip(self.fields()).all(|(n, f)| *n == f.name);
        if identity {
            return self;
        }
        let fields = exprs
            .iter()
            .zip(names)
            .map(|(e, n)| Field::new(n, e.ty().clone()))
            .collect();
        Rel::Project {
            input: Box::new(self),
            exprs,
            fields,
        }
    }

    /// The plain input references to every field of `self`.
    pub fn field_refs(&self) -> Vec<ScalarExpr> {
        self.fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ScalarExpr::input(i, f.ty.clone()))
            .collect()
    }

    /// The node's name in Flink's plans.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Rel::Scan { .. } => "TableSourceScan",
            Rel::Values { .. } => "Values",
            Rel::Project { .. } => "Project",
            Rel::Filter { .. } => "Filter",
            Rel::Watermark { .. } => "WatermarkAssigner",
            Rel::WindowTvf { .. } => "WindowTableFunction",
            Rel::Aggregate { .. } => "Aggregate",
            Rel::Join { .. } => "Join",
            Rel::Union { .. } => "Union",
            Rel::Rank { .. } => "Rank",
            Rel::Sort { .. } => "Sort",
        }
    }
}

/// A planned `INSERT`: the sink table and the query feeding it, already cast to the sink's types.
#[derive(Debug, Clone, PartialEq)]
pub struct SinkPlan {
    pub table: TableRef,
    pub input: Rel,
}
