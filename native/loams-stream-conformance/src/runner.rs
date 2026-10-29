//! Runs a case on a target and collects its result.
//!
//! A target is a SQL Gateway URL plus a directory both sides see: the
//! harness writes case data there and reads sink files back, the target
//! reads and writes it under its own path (`Shared`). For the oracle the
//! directory is `dev/oracle/work`, mounted at `/oracle/work`.
//!
//! Per case the runner opens a session with the deterministic defaults
//! (UTC, parallelism 1, `table.dml-sync`), runs `setup.sql` statement by
//! statement, runs `query.sql`, collects the result from the gateway's
//! result stream or from the sink's `debezium-json` files, and closes the
//! session.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::case::{Case, Collect, RestoreEnd, substitute};
use crate::client::{ExecuteStatement, FetchLimits, GatewayClient, OpenSession};
use crate::error::{Error, Result};
use crate::result::{Column, ResultTable, Row, RowKind};

/// A directory the harness and the target both see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shared {
    /// The harness's path.
    pub local: PathBuf,
    /// The target's path (e.g. `/oracle/work`).
    pub remote: String,
}

/// Something the harness runs cases on.
#[derive(Debug, Clone)]
pub struct Target {
    /// A short name (`flink`, `loams`), used in triage.
    pub name: String,
    /// The gateway client.
    pub client: GatewayClient,
    /// The shared directory.
    pub shared: Shared,
    /// Artefacts a case may require (`requires`), by name: the jar path as the target sees it.
    pub jars: BTreeMap<String, String>,
}

/// Session defaults every case starts from (a case's `[config]` overrides
/// them).
pub fn session_defaults() -> BTreeMap<String, String> {
    [
        ("table.local-time-zone", "UTC"),
        ("parallelism.default", "1"),
        ("table.dml-sync", "true"),
        ("table.exec.resource.default-parallelism", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect()
}

/// Runs `case` on `target`. `run_id` keeps runs apart in the shared
/// directory; the case's work directory is removed afterwards unless
/// `keep` is set.
///
/// # Errors
///
/// The first failing statement, collection or file error.
pub async fn run_case(
    target: &Target,
    case: &Case,
    run_id: &str,
    keep: bool,
) -> Result<ResultTable> {
    let rel = format!("runs/{run_id}/{}/{}", target.name, case.name);
    let local = target.shared.local.join(&rel);
    let remote = format!("{}/{rel}", target.shared.remote.trim_end_matches('/'));
    let local_data = local.join("data");
    let local_sink = local.join("sink");
    std::fs::create_dir_all(&local_sink).map_err(|e| Error::io(local_sink.display(), e))?;
    std::fs::create_dir_all(&local_data).map_err(|e| Error::io(local_data.display(), e))?;
    let case_data = case.dir.join("data");
    if case_data.is_dir() {
        copy_dir(&case_data, &local_data)?;
    }
    for need in &case.spec.requires {
        if !target.jars.contains_key(need) {
            return Err(Error::Skipped(format!(
                "case {} requires {need}, which target {} does not provide",
                case.name, target.name
            )));
        }
    }
    let result = run_in_session(
        target,
        case,
        &format!("{remote}/data"),
        &format!("{remote}/sink"),
        &local_data,
        &local_sink,
    )
    .await;
    if !keep {
        let _ = std::fs::remove_dir_all(&local);
    }
    match (&case.spec.expect_error, result) {
        (None, r) => r,
        (Some(want), Ok(_)) => Err(Error::Invalid(format!(
            "case {}: expected the error {want:?}, but the run succeeded",
            case.name
        ))),
        (Some(want), Err(e)) => {
            let text = format!("{e}\n{}", e.summary());
            let full = match &e {
                Error::Api { errors, .. } => errors.join("\n"),
                _ => String::new(),
            };
            if text.contains(want.as_str()) || full.contains(want.as_str()) {
                Ok(ResultTable {
                    columns: vec![Column::new("error", "STRING")],
                    rows: vec![Row::new(
                        RowKind::Insert,
                        vec![serde_json::to_string(want).expect("a string serialises")],
                    )],
                })
            } else {
                Err(e)
            }
        }
    }
}

/// A restore case's monitored filesystem source emits a watermark per row, as Flink's test
/// `values` source does: a periodic watermark can miss a file that is read in one go, and the
/// event-time results the program expects before the savepoint would never fire.
fn per_event_watermarks(stmt: &str) -> String {
    const MONITOR: &str = "'source.monitor-interval' = '200ms'";
    if stmt.contains(MONITOR) && !stmt.contains("scan.watermark.emit.strategy") {
        stmt.replace(
            MONITOR,
            "'source.monitor-interval' = '200ms',\n  'scan.watermark.emit.strategy' = 'on-event'",
        )
    } else {
        stmt.to_owned()
    }
}

/// Change events in the committed sink files under `dir` (0 when there are none yet).
fn sink_events(dir: &Path) -> usize {
    let mut files = Vec::new();
    if list_files_with(dir, &mut files, true).is_err() {
        return 0;
    }
    files
        .iter()
        .filter_map(|f| std::fs::read_to_string(f).ok())
        .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count())
        .sum()
}

/// Waits until the sink holds at least `want` change events, or (when `want` is unknown) until
/// it has stopped growing for `quiet`; fails after `timeout`.
async fn wait_for_events(
    dir: &Path,
    want: Option<usize>,
    timeout: Duration,
    case: &str,
) -> Result<usize> {
    let quiet = Duration::from_secs(3);
    let start = std::time::Instant::now();
    let mut last = (0usize, std::time::Instant::now());
    loop {
        let n = sink_events(dir);
        match want {
            Some(w) if n >= w => return Ok(n),
            None if n > 0 && n == last.0 && last.1.elapsed() >= quiet => return Ok(n),
            _ => {}
        }
        if n != last.0 {
            last = (n, std::time::Instant::now());
        }
        if start.elapsed() > timeout {
            return Err(Error::Timeout(format!(
                "case {case}: the sink holds {n} change events, waiting for {}",
                want.map_or("quiescence".to_owned(), |w| w.to_string())
            )));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Waits until the sink has not grown for `quiet` (bounded by `timeout`).
async fn wait_until_quiet(dir: &Path, quiet: Duration, timeout: Duration) {
    let start = std::time::Instant::now();
    let mut last = (sink_events(dir), std::time::Instant::now());
    while start.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let n = sink_events(dir);
        if n != last.0 {
            last = (n, std::time::Instant::now());
        } else if last.1.elapsed() >= quiet {
            return;
        }
    }
}

/// The first field of the first row, as a string (the job id of an async `INSERT`, the
/// savepoint path of `STOP JOB ... WITH SAVEPOINT`).
fn first_string(table: &ResultTable, what: &str) -> Result<String> {
    let field = table
        .rows
        .first()
        .and_then(|r| r.fields.first())
        .ok_or_else(|| Error::Invalid(format!("no {what} in the result")))?;
    serde_json::from_str::<String>(field)
        .map_err(|e| Error::Invalid(format!("{what} is not a string: {e}")))
}

async fn run_in_session(
    target: &Target,
    case: &Case,
    case_dir: &str,
    sink_dir: &str,
    local_data: &Path,
    local_sink: &Path,
) -> Result<ResultTable> {
    let client = &target.client;
    let mut properties = session_defaults();
    properties.insert(
        "execution.runtime-mode".into(),
        case.spec.mode.runtime_mode().into(),
    );
    if case.spec.restore {
        // The restore flow drives the job itself: asynchronous DML, and checkpoints so the
        // filesystem sink commits files while the job runs.
        properties.insert("table.dml-sync".into(), "false".into());
        properties.insert("execution.checkpointing.interval".into(), "500ms".into());
    }
    properties.extend(case.spec.config.clone());
    let session = client
        .open_session(&OpenSession {
            session_name: Some(format!("conformance-{}", case.name)),
            properties,
        })
        .await?;
    let limits = FetchLimits {
        timeout: Duration::from_secs(case.spec.timeout_secs),
        ..FetchLimits::default()
    };
    let outcome = async {
        for need in &case.spec.requires {
            let jar = &target.jars[need];
            client
                .execute_and_collect(
                    &session,
                    &ExecuteStatement::new(format!("ADD JAR '{jar}'")),
                    limits,
                )
                .await?;
        }
        for stmt in &case.setup {
            let mut sql = substitute(stmt, case_dir, sink_dir)?;
            if case.spec.restore {
                sql = per_event_watermarks(&sql);
            }
            client
                .execute_and_collect(&session, &ExecuteStatement::new(sql), limits)
                .await?;
        }
        let query = substitute(&case.query, case_dir, sink_dir)?;
        if case.spec.restore {
            return run_restore(
                target, case, &session, &query, local_data, local_sink, limits,
            )
            .await;
        }
        let got = client
            .execute_and_collect(&session, &ExecuteStatement::new(query), limits)
            .await?;
        match case.spec.collect {
            Collect::Query => {
                if got.truncated {
                    return Err(Error::Invalid(format!(
                        "case {}: more than {} rows",
                        case.name, limits.max_rows
                    )));
                }
                Ok(got.table)
            }
            Collect::Sink => {
                let sink = case.spec.sink.as_deref().expect("checked at load");
                let columns = describe(client, &session, sink, limits).await?;
                let rows = read_debezium_dir(local_sink, &columns)?;
                Ok(ResultTable { columns, rows })
            }
        }
    }
    .await;
    let _ = client.close_session(&session).await;
    outcome
}

/// The stop-with-savepoint and restore flow (`restore = true`).
async fn run_restore(
    target: &Target,
    case: &Case,
    session: &crate::client::SessionHandle,
    query: &str,
    local_data: &Path,
    local_sink: &Path,
    limits: FetchLimits,
) -> Result<ResultTable> {
    let client = &target.client;
    let timeout = Duration::from_secs(case.spec.timeout_secs);
    let before = case.spec.restore_before_rows.expect("checked at load");
    let after = case.spec.expected_rows().map(|(_, a)| a);
    let run = |sql: String| async move {
        client
            .execute_and_collect(session, &ExecuteStatement::new(sql), limits)
            .await
    };
    // Phase 1: the original data, then a savepoint.
    let job = first_string(&run(query.to_owned()).await?.table, "job id")?;
    if before > 0
        && let Err(e) = wait_for_events(local_sink, Some(before), timeout, &case.name).await
    {
        let _ = run(format!("STOP JOB '{job}'")).await;
        return Err(e);
    }
    let stopped = run(format!("STOP JOB '{job}' WITH SAVEPOINT")).await?;
    let savepoint = first_string(&stopped.table, "savepoint path")?;
    // Phase 2: the data that arrives after the restore, then the restored job.
    let more = case.dir.join("data-after-restore");
    if more.is_dir() {
        copy_dir_renamed(&more, local_data, "after-restore-")?;
    }
    run(format!(
        "SET 'execution.state-recovery.path' = '{savepoint}'"
    ))
    .await?;
    let job = first_string(&run(query.to_owned()).await?.table, "job id")?;
    let total = after.map(|a| before + a);
    let end_mode = case.spec.restore_end.unwrap_or(RestoreEnd::Drain);
    match (end_mode, total) {
        // A drained job fires its remaining timers and windows at the end, so the expected count
        // is only reached after the drain: wait until the sink is quiet, then drain.
        (RestoreEnd::Drain, _) => {
            wait_until_quiet(local_sink, Duration::from_secs(3), timeout).await;
        }
        (RestoreEnd::Cancel, Some(t)) if t > before => {
            if let Err(e) = wait_for_events(local_sink, Some(t), timeout, &case.name).await {
                let _ = run(format!("STOP JOB '{job}'")).await;
                return Err(e);
            }
        }
        _ => {
            // Nothing more is expected: give the restored job a moment to (not) emit.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    let end = match end_mode {
        RestoreEnd::Drain => format!("STOP JOB '{job}' WITH SAVEPOINT WITH DRAIN"),
        RestoreEnd::Cancel => format!("STOP JOB '{job}' WITH SAVEPOINT"),
    };
    run(end).await?;
    let sink = case.spec.sink.as_deref().expect("checked at load");
    let columns = describe(client, session, sink, limits).await?;
    let rows = read_debezium_files(local_sink, &columns, true)?;
    Ok(ResultTable { columns, rows })
}

/// Copies `from` into `to`, prefixing file names so they do not replace the files already there
/// (a monitoring filesystem source picks the new files up).
fn copy_dir_renamed(from: &Path, to: &Path, prefix: &str) -> Result<()> {
    std::fs::create_dir_all(to).map_err(|e| Error::io(to.display(), e))?;
    let entries = std::fs::read_dir(from).map_err(|e| Error::io(from.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(from.display(), e))?;
        let src = entry.path();
        if src.is_dir() {
            copy_dir_renamed(&src, &to.join(entry.file_name()), prefix)?;
        } else {
            let dst = to.join(format!("{prefix}{}", entry.file_name().to_string_lossy()));
            // Write to a hidden name first, then rename: the source must not read half a file.
            let tmp = to.join(format!(".{prefix}{}", entry.file_name().to_string_lossy()));
            std::fs::copy(&src, &tmp).map_err(|e| Error::io(src.display(), e))?;
            std::fs::rename(&tmp, &dst).map_err(|e| Error::io(dst.display(), e))?;
        }
    }
    Ok(())
}

/// The columns of `table` from `DESCRIBE`: `name`, and the type with
/// ` NOT NULL` when the column is not nullable.
async fn describe(
    client: &GatewayClient,
    session: &crate::client::SessionHandle,
    table: &str,
    limits: FetchLimits,
) -> Result<Vec<Column>> {
    if !table
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(Error::Invalid(format!("bad sink table name {table:?}")));
    }
    let got = client
        .execute_and_collect(
            session,
            &ExecuteStatement::new(format!("DESCRIBE {table}")),
            limits,
        )
        .await?;
    got.table
        .rows
        .iter()
        .map(|r| {
            let text = |i: usize| -> Result<String> {
                let f = r.fields.get(i).ok_or_else(|| Error::Decode {
                    url: format!("DESCRIBE {table}"),
                    message: format!("row {r} has no field {i}"),
                })?;
                serde_json::from_str::<String>(f).map_err(|e| Error::Decode {
                    url: format!("DESCRIBE {table}"),
                    message: e.to_string(),
                })
            };
            let nullable = r.fields.get(2).map(String::as_str) != Some("false");
            let ty = text(1)?;
            Ok(Column::new(
                text(0)?,
                if nullable {
                    ty
                } else {
                    format!("{ty} NOT NULL")
                },
            ))
        })
        .collect()
}

#[derive(Deserialize)]
struct Debezium<'a> {
    #[serde(borrow, default)]
    before: Option<BTreeMap<String, &'a RawValue>>,
    #[serde(borrow, default)]
    after: Option<BTreeMap<String, &'a RawValue>>,
    op: String,
}

/// Reads every committed `debezium-json` file under `dir` (hidden and
/// in-progress files skipped), in path order, into changelog rows.
///
/// # Errors
///
/// [`Error::Io`], or [`Error::Decode`] for a line that is not a change
/// event over `columns`.
pub fn read_debezium_dir(dir: &Path, columns: &[Column]) -> Result<Vec<Row>> {
    read_debezium_files(dir, columns, false)
}

/// As [`read_debezium_dir`]; with `in_progress`, also the hidden in-progress part files. A
/// row-format filesystem sink only finalises a part file when its rolling policy says so, so a
/// job that is stopped (not drained) leaves its rows there; Flink truncates them to the last
/// checkpoint on restore, so their content is exactly the committed changelog.
pub fn read_debezium_files(dir: &Path, columns: &[Column], in_progress: bool) -> Result<Vec<Row>> {
    let mut files = Vec::new();
    list_files_with(dir, &mut files, in_progress)?;
    files.sort();
    let mut rows = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).map_err(|e| Error::io(file.display(), e))?;
        for (n, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let at = || format!("{}:{}", file.display(), n + 1);
            let ev: Debezium<'_> = serde_json::from_str(line).map_err(|e| Error::Decode {
                url: at(),
                message: e.to_string(),
            })?;
            let image = |m: &Option<BTreeMap<String, &RawValue>>| -> Result<Vec<String>> {
                let m = m.as_ref().ok_or_else(|| Error::Decode {
                    url: at(),
                    message: format!("op {:?} without its row image", ev.op),
                })?;
                if m.len() != columns.len() {
                    return Err(Error::Decode {
                        url: at(),
                        message: format!("{} fields for {} columns", m.len(), columns.len()),
                    });
                }
                columns
                    .iter()
                    .map(|c| {
                        m.get(&c.name)
                            .map(|v| v.get().to_owned())
                            .ok_or_else(|| Error::Decode {
                                url: at(),
                                message: format!("no field {:?}", c.name),
                            })
                    })
                    .collect()
            };
            match ev.op.as_str() {
                "c" | "r" => rows.push(Row::new(RowKind::Insert, image(&ev.after)?)),
                "d" => rows.push(Row::new(RowKind::Delete, image(&ev.before)?)),
                "u" => {
                    rows.push(Row::new(RowKind::UpdateBefore, image(&ev.before)?));
                    rows.push(Row::new(RowKind::UpdateAfter, image(&ev.after)?));
                }
                other => {
                    return Err(Error::Decode {
                        url: at(),
                        message: format!("unknown op {other:?}"),
                    });
                }
            }
        }
    }
    Ok(rows)
}

fn list_files_with(dir: &Path, out: &mut Vec<PathBuf>, in_progress: bool) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| Error::io(dir.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(dir.display(), e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let hidden = name.starts_with(['.', '_']);
        let path = entry.path();
        if path.is_dir() {
            if !hidden {
                list_files_with(&path, out, in_progress)?;
            }
        } else if !hidden
            || (in_progress && name.starts_with(".part-") && name.contains(".inprogress"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to).map_err(|e| Error::io(to.display(), e))?;
    let entries = std::fs::read_dir(from).map_err(|e| Error::io(from.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(from.display(), e))?;
        let src = entry.path();
        let dst = to.join(entry.file_name());
        if src.is_dir() {
            copy_dir(&src, &dst)?;
        } else {
            std::fs::copy(&src, &dst).map_err(|e| Error::io(src.display(), e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debezium_events_become_changelog_rows() {
        let dir = std::env::current_dir()
            .unwrap()
            .join("target-test-debezium");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("p")).unwrap();
        std::fs::write(
            dir.join("p/part-0"),
            "{\"before\":null,\"after\":{\"k\":1,\"v\":1.0E20},\"op\":\"c\"}\n\
             {\"before\":{\"k\":1,\"v\":1.0E20},\"after\":{\"k\":1,\"v\":0.10},\"op\":\"u\"}\n\
             {\"before\":{\"k\":1,\"v\":0.10},\"after\":null,\"op\":\"d\"}\n",
        )
        .unwrap();
        std::fs::write(dir.join("p/.part-1.inprogress"), "garbage").unwrap();
        let cols = [Column::new("k", "INT"), Column::new("v", "DOUBLE")];
        let rows = read_debezium_dir(&dir, &cols);
        std::fs::remove_dir_all(&dir).unwrap();
        let rows = rows.unwrap();
        let kinds: Vec<_> = rows.iter().map(|r| r.kind).collect();
        assert_eq!(
            kinds,
            [
                RowKind::Insert,
                RowKind::UpdateBefore,
                RowKind::UpdateAfter,
                RowKind::Delete
            ]
        );
        assert_eq!(rows[0].fields, ["1", "1.0E20"]);
        assert_eq!(rows[2].fields, ["1", "0.10"]);
    }
}
