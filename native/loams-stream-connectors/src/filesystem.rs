//! The `filesystem` connector over local paths: bounded reads of a table's files and a sink
//! writing part files (FC1 Task 10; the exactly-once two-phase sink is Task 18).

use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;

use crate::format::FormatError;
use crate::format::json::{self, Column};

/// The files of a table path in name order (hidden and `_` files skipped, as Flink's
/// enumerator does), or the path itself if it is a file.
pub fn table_files(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_owned()]);
    }
    let mut out = vec![];
    collect(path, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(['.', '_']) {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

/// Reads one file of a table in `format` into a batch of `columns`.
pub fn read_file(
    path: &Path,
    format: &str,
    columns: &[Column],
) -> Result<RecordBatch, FormatError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| FormatError(format!("{}: {e}", path.display())))?;
    match format {
        "json" => json::decode_json(&text, columns),
        "debezium-json" => json::decode_debezium(&text, columns),
        other => Err(FormatError(format!(
            "the filesystem connector of Loams Stream does not read format '{other}' yet"
        ))),
    }
}

/// Encodes a batch in `format` as lines.
pub fn encode(
    batch: &RecordBatch,
    format: &str,
    columns: &[Column],
) -> Result<Vec<String>, FormatError> {
    match format {
        "json" => json::encode_json(batch, columns),
        "debezium-json" => json::encode_debezium(batch, columns),
        other => Err(FormatError(format!(
            "the filesystem connector of Loams Stream does not write format '{other}' yet"
        ))),
    }
}
