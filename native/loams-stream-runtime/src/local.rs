//! A single-threaded executor of a physical plan over bounded sources (FC1 Task 10).
//!
//! It runs a plan end to end in one process, without task managers, the network or checkpoints:
//! the planner's tests and the corpus run through it until the runtime of Task 11 replaces it.
//! Sources read their files to the end; each source batch is followed by its watermark; the end
//! of input fires every timer (Flink's `MAX_WATERMARK`) and flushes every operator.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array, RecordBatch, RecordBatchOptions};
use arrow::datatypes::Schema;
use loams_stream_connectors::filesystem;
use loams_stream_connectors::format::json::{self as json_format, Column};
use loams_stream_plan::expr::Literal;
use loams_stream_plan::physical::{ConnectorTable, NodeId, PhysicalOp, PhysicalPlan};

use crate::driver::{Driver, DriverError};

/// Why a job failed.
#[derive(Debug, Clone, PartialEq)]
pub struct JobError(pub String);

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for JobError {}

fn job_err(e: impl std::fmt::Display) -> JobError {
    JobError(e.to_string())
}

/// How to run a job.
#[derive(Debug, Clone)]
pub struct LocalOptions {
    /// Rows per source batch.
    pub batch_rows: usize,
    /// Processing time (`now_ms`) for operators that read the clock.
    pub now_ms: i64,
}

impl Default for LocalOptions {
    fn default() -> Self {
        LocalOptions {
            batch_rows: 1024,
            now_ms: 0,
        }
    }
}

/// What a finished job produced: the rows of each `collect` sink, by sink node.
#[derive(Debug, Default)]
pub struct JobOutput {
    pub collected: BTreeMap<NodeId, Vec<RecordBatch>>,
}

enum Event {
    Batch(RecordBatch),
    Watermark(i64),
    End,
}

/// The per-node runtime state.
enum Task {
    Source,
    Values,
    Watermark {
        rowtime: usize,
        delay_ms: i64,
        current: i64,
    },
    Native(Box<Driver>),
    Union,
    Enforcer(loams_stream_plan::physical::EnforcerSpec),
    Sink {
        table: ConnectorTable,
        batches: Vec<RecordBatch>,
    },
}

struct NodeState {
    task: Task,
    /// The watermark and end-of-input of each input.
    input_watermarks: Vec<i64>,
    input_ended: Vec<bool>,
    emitted_watermark: i64,
    ended: bool,
}

/// Runs `plan` to the end of its bounded sources.
pub fn run(plan: &PhysicalPlan, options: &LocalOptions) -> Result<JobOutput, JobError> {
    let mut states = vec![];
    for node in &plan.nodes {
        let task = match &node.op {
            PhysicalOp::Source(_) => Task::Source,
            PhysicalOp::Values(_) => Task::Values,
            PhysicalOp::Watermark(w) => Task::Watermark {
                rowtime: w.rowtime,
                delay_ms: w.delay_ms,
                current: i64::MIN,
            },
            PhysicalOp::Native(spec) => {
                let mut d = guarded(|| Driver::new(spec))?;
                d.set_now(options.now_ms);
                Task::Native(Box::new(d))
            }
            PhysicalOp::Union => Task::Union,
            PhysicalOp::ConstraintEnforcer(spec) => Task::Enforcer(spec.clone()),
            PhysicalOp::Sink(t) => Task::Sink {
                table: t.clone(),
                batches: vec![],
            },
        };
        states.push(NodeState {
            task,
            input_watermarks: vec![i64::MIN; node.inputs.len()],
            input_ended: vec![false; node.inputs.len()],
            emitted_watermark: i64::MIN,
            ended: false,
        });
    }
    let mut exec = Executor {
        plan,
        states,
        output: JobOutput::default(),
    };
    // Read every source, then interleave their batches round-robin.
    let mut queues: Vec<(NodeId, std::collections::VecDeque<RecordBatch>)> = vec![];
    for node in &plan.nodes {
        match &node.op {
            PhysicalOp::Source(table) => {
                queues.push((node.id, read_source(table, options)?.into()));
            }
            PhysicalOp::Values(rows) => {
                queues.push((node.id, vec![values_batch(rows, &node.fields)?].into()));
            }
            _ => {}
        }
    }
    loop {
        let mut progressed = false;
        for (id, queue) in queues.iter_mut() {
            if let Some(batch) = queue.pop_front() {
                progressed = true;
                exec.emit(*id, Event::Batch(batch))?;
            }
        }
        if !progressed {
            break;
        }
    }
    for (id, _) in &queues {
        exec.emit(*id, Event::End)?;
    }
    Ok(exec.output)
}

struct Executor<'a> {
    plan: &'a PhysicalPlan,
    states: Vec<NodeState>,
    output: JobOutput,
}

fn guarded<T>(f: impl FnOnce() -> Result<T, DriverError>) -> Result<T, JobError> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r.map_err(job_err),
        Err(panic) => Err(JobError(
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_else(|| "operator panicked".into()),
        )),
    }
}

impl Executor<'_> {
    /// Sends `event` from `from` to each consumer.
    fn emit(&mut self, from: NodeId, event: Event) -> Result<(), JobError> {
        let consumers: Vec<(NodeId, usize)> = self
            .plan
            .nodes
            .iter()
            .flat_map(|n| {
                n.inputs
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| e.node == from)
                    .map(move |(i, _)| (n.id, i))
            })
            .collect();
        match event {
            Event::Batch(b) => {
                for (to, input) in consumers {
                    self.receive(to, input, Event::Batch(b.clone()))?;
                }
            }
            Event::Watermark(w) => {
                for (to, input) in consumers {
                    self.receive(to, input, Event::Watermark(w))?;
                }
            }
            Event::End => {
                for (to, input) in consumers {
                    self.receive(to, input, Event::End)?;
                }
            }
        }
        Ok(())
    }

    fn emit_all(&mut self, from: NodeId, batches: Vec<RecordBatch>) -> Result<(), JobError> {
        for b in batches {
            if b.num_rows() > 0 {
                self.emit(from, Event::Batch(b))?;
            }
        }
        Ok(())
    }

    fn receive(&mut self, id: NodeId, input: usize, event: Event) -> Result<(), JobError> {
        match event {
            Event::Batch(batch) => self.on_batch(id, input, batch),
            Event::Watermark(w) => {
                let state = &mut self.states[id];
                if w <= state.input_watermarks[input] {
                    return Ok(());
                }
                state.input_watermarks[input] = w;
                let combined = *state.input_watermarks.iter().min().expect("an input");
                if combined > state.emitted_watermark {
                    state.emitted_watermark = combined;
                    self.on_watermark(id, combined)?;
                }
                Ok(())
            }
            Event::End => {
                let state = &mut self.states[id];
                state.input_ended[input] = true;
                if state.input_ended.iter().all(|e| *e) && !state.ended {
                    state.ended = true;
                    self.on_end(id)?;
                }
                Ok(())
            }
        }
    }

    fn on_batch(&mut self, id: NodeId, input: usize, batch: RecordBatch) -> Result<(), JobError> {
        let mut forward = None;
        let mut out = vec![];
        let mut watermark = None;
        match &mut self.states[id].task {
            Task::Source | Task::Values => {}
            Task::Watermark {
                rowtime,
                delay_ms,
                current,
            } => {
                if let Some(max) = max_millis(batch.column(*rowtime)) {
                    let w = max.saturating_sub(*delay_ms);
                    if w > *current {
                        *current = w;
                        watermark = Some(w);
                    }
                }
                forward = Some(batch);
            }
            Task::Native(driver) => {
                out = guarded(|| driver.push(input, batch))?;
            }
            Task::Union => forward = Some(batch),
            Task::Enforcer(spec) => {
                forward = Some(crate::enforcer::enforce(batch, spec).map_err(JobError)?);
            }
            Task::Sink { batches, .. } => batches.push(batch),
        }
        if let Some(b) = forward {
            self.emit(id, Event::Batch(b))?;
        }
        self.emit_all(id, out)?;
        if let Some(w) = watermark {
            self.emit(id, Event::Watermark(w))?;
        }
        Ok(())
    }

    fn on_watermark(&mut self, id: NodeId, watermark: i64) -> Result<(), JobError> {
        let mut out = vec![];
        match &mut self.states[id].task {
            // A watermark assigner makes its own watermarks; upstream ones stop there.
            Task::Watermark { .. } | Task::Sink { .. } => return Ok(()),
            Task::Native(driver) => {
                out = guarded(|| driver.watermark(watermark))?;
            }
            _ => {}
        }
        self.emit_all(id, out)?;
        self.emit(id, Event::Watermark(watermark))
    }

    fn on_end(&mut self, id: NodeId) -> Result<(), JobError> {
        let mut out = vec![];
        match &mut self.states[id].task {
            Task::Native(driver) => {
                out = guarded(|| driver.finish())?;
            }
            Task::Sink { table, batches } => {
                let batches = std::mem::take(batches);
                let table = table.clone();
                return self.finish_sink(id, &table, batches);
            }
            _ => {}
        }
        self.emit_all(id, out)?;
        self.emit(id, Event::Watermark(i64::MAX))?;
        self.emit(id, Event::End)
    }

    fn finish_sink(
        &mut self,
        id: NodeId,
        table: &ConnectorTable,
        batches: Vec<RecordBatch>,
    ) -> Result<(), JobError> {
        match table.connector() {
            "collect" => {
                self.output.collected.insert(id, batches);
                Ok(())
            }
            "blackhole" => Ok(()),
            "filesystem" => {
                let path = table
                    .options
                    .get("path")
                    .ok_or_else(|| JobError("filesystem sink without 'path'".into()))?;
                let format = table.format().unwrap_or("");
                let columns = columns(table);
                let mut lines = vec![];
                for b in &batches {
                    lines.extend(filesystem::encode(b, format, &columns).map_err(job_err)?);
                }
                let dir = local_path(path);
                std::fs::create_dir_all(&dir).map_err(job_err)?;
                if !lines.is_empty() {
                    let mut text = lines.join("\n");
                    text.push('\n');
                    std::fs::write(dir.join("part-0-0"), text).map_err(job_err)?;
                }
                Ok(())
            }
            other => Err(JobError(format!(
                "the local executor does not write connector '{other}'"
            ))),
        }
    }
}

fn columns(table: &ConnectorTable) -> Vec<Column> {
    table
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.ty.clone()))
        .collect()
}

fn local_path(path: &str) -> PathBuf {
    PathBuf::from(path.strip_prefix("file://").unwrap_or(path))
}

/// Reads a bounded source into batches of at most `batch_rows` rows.
fn read_source(
    table: &ConnectorTable,
    options: &LocalOptions,
) -> Result<Vec<RecordBatch>, JobError> {
    match table.connector() {
        "filesystem" => {
            let path = table
                .options
                .get("path")
                .ok_or_else(|| JobError("filesystem source without 'path'".into()))?;
            let format = table.format().unwrap_or("");
            let columns = columns(table);
            let mut out = vec![];
            for file in filesystem::table_files(Path::new(&local_path(path))).map_err(job_err)? {
                let batch = filesystem::read_file(&file, format, &columns).map_err(job_err)?;
                let mut offset = 0;
                while offset < batch.num_rows() {
                    let len = options.batch_rows.min(batch.num_rows() - offset);
                    out.push(batch.slice(offset, len));
                    offset += len;
                }
            }
            Ok(out)
        }
        "datagen" => datagen(table, options),
        other => Err(JobError(format!(
            "the local executor does not read connector '{other}'"
        ))),
    }
}

/// A bounded `datagen` source whose fields are all `sequence`s (Flink's `SequenceGenerator`):
/// field `f` runs from `fields.f.start` to `fields.f.end`; `number-of-rows` caps the count.
/// Random fields are not deterministic and are refused (FC1 ruling R5: deterministic sources).
fn datagen(table: &ConnectorTable, options: &LocalOptions) -> Result<Vec<RecordBatch>, JobError> {
    let mut ranges = vec![];
    for f in &table.fields {
        let opt = |k: &str| table.options.get(&format!("fields.{}.{k}", f.name));
        if opt("kind").map(String::as_str) != Some("sequence") {
            return Err(JobError(format!(
                "datagen field '{}' is not a sequence; random datagen fields are not deterministic",
                f.name
            )));
        }
        let parse = |k: &str| -> Result<i64, JobError> {
            opt(k)
                .ok_or_else(|| JobError(format!("datagen field '{}' needs '{k}'", f.name)))?
                .trim()
                .parse()
                .map_err(job_err)
        };
        ranges.push((parse("start")?, parse("end")?));
    }
    let mut rows = ranges
        .iter()
        .map(|(start, end)| (end - start + 1).max(0))
        .min()
        .unwrap_or(0);
    if let Some(n) = table.options.get("number-of-rows") {
        rows = rows.min(n.trim().parse().map_err(job_err)?);
    }
    let columns = columns(table);
    let mut text = String::new();
    for i in 0..rows {
        let mut obj = serde_json::Map::new();
        for ((name, ty), (start, _)) in columns.iter().zip(&ranges) {
            let v = start + i;
            let value = match ty.kind {
                loams_stream_sql::types::LogicalKind::Char(_)
                | loams_stream_sql::types::LogicalKind::Varchar(_) => {
                    serde_json::Value::from(v.to_string())
                }
                _ => serde_json::Value::from(v),
            };
            obj.insert(name.clone(), value);
        }
        text.push_str(&serde_json::Value::Object(obj).to_string());
        text.push('\n');
    }
    let batch = json_format::decode_json(&text, &columns).map_err(job_err)?;
    let mut out = vec![];
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = options.batch_rows.min(batch.num_rows() - offset);
        out.push(batch.slice(offset, len));
        offset += len;
    }
    Ok(out)
}

/// The largest epoch millisecond of a timestamp column.
fn max_millis(column: &ArrayRef) -> Option<i64> {
    use arrow::array::{Array, AsArray};
    let s = column.as_struct_opt()?;
    let millis = s.column(0).as_primitive::<arrow::datatypes::Int64Type>();
    (0..s.len())
        .filter(|i| s.is_valid(*i))
        .map(|i| millis.value(i))
        .max()
}

/// A batch of constant rows.
fn values_batch(
    rows: &[Vec<Literal>],
    fields: &[loams_stream_plan::logical::Field],
) -> Result<RecordBatch, JobError> {
    let columns: Vec<Column> = fields
        .iter()
        .map(|f| (f.name.clone(), f.ty.clone()))
        .collect();
    if fields.is_empty() {
        return RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(job_err);
    }
    // Write the rows as JSON and read them back with the json format's types.
    let mut text = String::new();
    for row in rows {
        let mut obj = serde_json::Map::new();
        for ((name, ty), v) in columns.iter().zip(row) {
            obj.insert(name.clone(), literal_json(v, ty)?);
        }
        text.push_str(&serde_json::Value::Object(obj).to_string());
        text.push('\n');
    }
    json_format::decode_json(&text, &columns).map_err(job_err)
}

fn literal_json(
    v: &Literal,
    ty: &loams_stream_sql::types::LogicalType,
) -> Result<serde_json::Value, JobError> {
    use serde_json::Value;
    Ok(match v {
        Literal::Null => Value::Null,
        Literal::Boolean(b) => Value::Bool(*b),
        Literal::Integer(i) => Value::from(*i),
        Literal::Double(d) => Value::from(*d),
        Literal::String(s) => Value::from(s.clone()),
        Literal::Decimal { unscaled, scale } => {
            Value::from(json_format::java_decimal(*unscaled, *scale as i8))
        }
        other => {
            return Err(JobError(format!(
                "VALUES literal {other:?} of type {ty} in the local executor"
            )));
        }
    })
}

/// The row kinds of a batch as Flink's short strings.
pub fn row_kinds(batch: &RecordBatch) -> Vec<&'static str> {
    use arrow::array::Array;
    match batch.column_by_name("$row_kind$") {
        Some(c) => {
            let kinds = c
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8 row kinds");
            (0..kinds.len())
                .map(|i| match kinds.value(i) {
                    1 => "-U",
                    2 => "+U",
                    3 => "-D",
                    _ => "+I",
                })
                .collect()
        }
        None => vec!["+I"; batch.num_rows()],
    }
}
