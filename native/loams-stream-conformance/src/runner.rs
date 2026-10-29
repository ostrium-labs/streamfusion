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

use crate::case::{Case, Collect, substitute};
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
    let result = run_in_session(
        target,
        case,
        &format!("{remote}/data"),
        &format!("{remote}/sink"),
        &local_sink,
    )
    .await;
    if !keep {
        let _ = std::fs::remove_dir_all(&local);
    }
    result
}

async fn run_in_session(
    target: &Target,
    case: &Case,
    case_dir: &str,
    sink_dir: &str,
    local_sink: &Path,
) -> Result<ResultTable> {
    let client = &target.client;
    let mut properties = session_defaults();
    properties.insert(
        "execution.runtime-mode".into(),
        case.spec.mode.runtime_mode().into(),
    );
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
        for stmt in &case.setup {
            let sql = substitute(stmt, case_dir, sink_dir)?;
            client
                .execute_and_collect(&session, &ExecuteStatement::new(sql), limits)
                .await?;
        }
        let query = substitute(&case.query, case_dir, sink_dir)?;
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
    let mut files = Vec::new();
    list_files(dir, &mut files)?;
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

fn list_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| Error::io(dir.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(dir.display(), e))?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with(['.', '_']) {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            list_files(&path, out)?;
        } else {
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
