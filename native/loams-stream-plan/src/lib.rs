//! The streaming planner of Loams Stream: bound Flink SQL to a physical plan of native operator
//! specs (FC1 Task 10; design 82 §7.1).
//!
//! - [`convert`] turns parsed statements into a logical plan ([`logical::Rel`]) with the binder's
//!   types;
//! - [`lower`] turns it into a [`physical::PhysicalPlan`]: native operators as [`OpSpec`]s with
//!   their expressions in the engine's encoding ([`encode`], the port of StreamFusion's Java
//!   encoder), plus the sources, watermark assigners, unions and sinks around them;
//! - [`changelog`] infers each node's changelog mode and checks the sinks;
//! - [`explain`] prints the plan in Flink's `EXPLAIN` shape.
//!
//! Part of Loams Stream, the Apache Flink-compatible engine (Loams design 82). **Preview**:
//! constructs the planner does not lower yet fail with a named feature (D2268).

pub mod changelog;
pub mod convert;
pub mod encode;
pub mod error;
pub mod explain;
pub mod expr;
pub mod logical;
pub mod lower;
pub mod opspec;
pub mod physical;

use std::collections::BTreeMap;

use loams_stream_sql::binder::{Binder, Bound};
use loams_stream_sql::{ExplainDetail, Statement};

pub use error::{PlanError, PlanResult};
pub use opspec::{
    CalcSpec, EncodedExpr, GroupAggSpec, IntervalJoinSpec, KeepFirstSpec, KeepLastSpec,
    OPSPEC_VERSION, OpSpec, SessionAggSpec, SortKey, TopNSpec, UpdatingJoinSpec, WindowAggSpec,
    WindowJoinSpec, WindowRankSpec, WindowShape, WindowTvfSpec,
};
pub use physical::PhysicalPlan;

use crate::convert::Converter;
use crate::logical::{Field, SinkPlan, TableRef};
use crate::lower::Lowering;

/// What planning a statement gave.
#[derive(Debug, Clone, PartialEq)]
pub enum Planned {
    /// A statement answered by the session (DDL, `SET`, `USE`, `SHOW`, ...).
    Session(Bound),
    /// A job: `INSERT`, a statement set, or a query (whose sink is the `collect` connector).
    Job(PhysicalPlan),
    /// `EXPLAIN`: the plan text.
    Explain(String),
}

/// One SQL session's planner: the binder's catalog state plus planning.
#[derive(Debug, Default)]
pub struct Planner {
    binder: Binder,
}

impl Planner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn binder(&self) -> &Binder {
        &self.binder
    }

    pub fn binder_mut(&mut self) -> &mut Binder {
        &mut self.binder
    }

    /// Plans every statement of a script.
    pub fn plan_script(&mut self, sql: &str) -> PlanResult<Vec<Planned>> {
        let statements = loams_stream_sql::parse(sql)?;
        statements.iter().map(|s| self.plan(s)).collect()
    }

    /// Binds `statement` (applying its session effects) and plans it.
    pub fn plan(&mut self, statement: &Statement) -> PlanResult<Planned> {
        let bound = self.binder.bind(statement)?;
        match statement {
            Statement::Query(q) => {
                let rel = Converter::new(&self.binder).query(&q.query)?;
                Ok(Planned::Job(self.lower(vec![collect_sink(rel)])?))
            }
            Statement::Insert(insert) => {
                let sink = Converter::new(&self.binder).insert(insert)?;
                Ok(Planned::Job(self.lower(vec![sink])?))
            }
            Statement::StatementSet(set) => {
                let mut sinks = vec![];
                for insert in &set.inserts {
                    sinks.push(Converter::new(&self.binder).insert(insert)?);
                }
                Ok(Planned::Job(self.lower(sinks)?))
            }
            Statement::Execute(e) => self.plan(&e.statement),
            Statement::Explain(e) => {
                let plan = match self.plan(&e.statement)? {
                    Planned::Job(plan) => plan,
                    other => return Ok(other),
                };
                let changelog = e
                    .details
                    .iter()
                    .any(|d| matches!(d, ExplainDetail::ChangelogMode));
                Ok(Planned::Explain(explain::explain(&plan, changelog)))
            }
            _ => Ok(Planned::Session(bound)),
        }
    }

    fn lower(&self, sinks: Vec<SinkPlan>) -> PlanResult<PhysicalPlan> {
        let mut lowering = Lowering::new();
        for sink in &sinks {
            lowering.sink(sink)?;
        }
        Ok(lowering.finish())
    }
}

/// A query's result sink: the gateway's `collect`.
fn collect_sink(rel: logical::Rel) -> SinkPlan {
    let physical: Vec<Field> = rel
        .fields()
        .iter()
        .map(|f| Field::new(&f.name, expr::materialized(&f.ty)))
        .collect();
    let exprs = rel
        .field_refs()
        .into_iter()
        .zip(&physical)
        .map(|(e, f)| convert::cast_to(e, &f.ty))
        .collect();
    let names = physical.iter().map(|f| f.name.clone()).collect();
    SinkPlan {
        table: TableRef {
            id: loams_stream_sql::binder::ObjectIdentifier {
                catalog: "default_catalog".into(),
                database: "default_database".into(),
                object: "collect".into(),
            },
            options: BTreeMap::from([("connector".to_owned(), "collect".to_owned())]),
            physical,
            primary_key: None,
        },
        input: rel.project(exprs, names),
    }
}
