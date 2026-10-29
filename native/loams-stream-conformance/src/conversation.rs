//! The scripted conversation behind the recorded gateway fixtures.
//!
//! `loams-stream-oracle fixtures` runs it against a live gateway once per
//! API version with a [`crate::client::Recorder`] attached and writes the
//! exchanges to `tests/fixtures/gateway/v<N>.json`; the test
//! `client_speaks_v1_to_v4` replays them and runs the same script, so the
//! client must send exactly the requests the live gateway answered.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::client::{
    ApiVersion, Exchange, ExecuteStatement, FetchLimits, GatewayClient, GatewayInfo, OpenSession,
    OperationStatus,
};
use crate::error::{Error, Result};
use crate::result::ResultTable;

/// The typed query the conversation runs: integers, doubles (whose text
/// form matters), a decimal, temporal types, NULL, and nested types.
pub const TYPED_QUERY: &str = "SELECT a, CAST(a AS DOUBLE) / 3 AS d, CAST(1.5 AS FLOAT) AS f, \
1.10 AS dc, CAST(1e20 AS DOUBLE) AS big, DATE '2024-01-01' AS dt, \
TIMESTAMP '2024-01-01 00:00:01.5' AS ts, TO_TIMESTAMP_LTZ(1000, 3) AS ltz, \
CAST(NULL AS STRING) AS nn, ARRAY[a, 2] AS arr, MAP['k', a] AS m, ROW(a, 'x') AS r \
FROM (VALUES (1), (2)) AS t(a)";

/// A query that must fail: the table does not exist.
pub const FAILING_QUERY: &str = "SELECT * FROM no_such_table";

/// What the conversation saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transcript {
    /// `GET /api_versions`.
    pub versions: Vec<ApiVersion>,
    /// `GET /info`.
    pub info: (String, String),
    /// The session's `table.local-time-zone`.
    pub session_time_zone: Option<String>,
    /// `complete-statement` candidates (v2+).
    pub candidates: Option<Vec<String>>,
    /// The typed query's status right after submission.
    pub first_status: OperationStatus,
    /// The typed query's result.
    pub result: ResultTable,
    /// Whether the typed query reported a job.
    pub had_job: bool,
    /// The failing query's error summary.
    pub failure: String,
    /// `DELETE /sessions/:h`'s status.
    pub closed: String,
}

/// Runs the conversation with `client` (at its version).
///
/// # Errors
///
/// Any unexpected call failure, or [`Error::Invalid`] when the failing
/// query succeeds.
pub async fn run(client: &GatewayClient, poll: Duration) -> Result<Transcript> {
    let versions = client.api_versions().await?;
    let GatewayInfo {
        product_name,
        version,
    } = client.info().await?;
    let session = client
        .open_session(&OpenSession {
            session_name: Some("fixtures".into()),
            properties: BTreeMap::from([
                ("table.local-time-zone".to_owned(), "UTC".to_owned()),
                ("execution.runtime-mode".to_owned(), "batch".to_owned()),
            ]),
        })
        .await?;
    let config = client.session_config(&session).await?;
    client.heartbeat(&session).await?;
    let candidates = if client.version() >= ApiVersion::V2 {
        client
            .configure_session(&session, "SET 'parallelism.default' = '1'")
            .await?;
        Some(
            client
                .complete_statement(&session, "SELECT * FR", 11)
                .await?,
        )
    } else {
        None
    };
    let limits = FetchLimits {
        poll,
        max_poll: poll,
        timeout: Duration::from_secs(120),
        ..FetchLimits::default()
    };
    let op = client
        .execute_statement(&session, &ExecuteStatement::new(TYPED_QUERY))
        .await?;
    let first_status = client.operation_status(&session, &op).await?;
    let got = client.fetch_all(&session, &op, limits).await?;
    client.close_operation(&session, &op).await?;
    let failure = match client
        .execute_and_collect(&session, &ExecuteStatement::new(FAILING_QUERY), limits)
        .await
    {
        Ok(_) => return Err(Error::Invalid(format!("{FAILING_QUERY} succeeded"))),
        Err(e) => e.summary(),
    };
    let closed = client.close_session(&session).await?;
    Ok(Transcript {
        versions,
        info: (product_name, version),
        session_time_zone: config.get("table.local-time-zone").cloned(),
        candidates,
        first_status,
        result: got.table,
        had_job: got.job_id.is_some(),
        failure,
        closed,
    })
}

/// Drops the Java stack frames (`\tat …` lines) from an error response's
/// `errors` strings, keeping the exception and `Caused by:` lines, so the
/// fixtures stay small. Successful responses are left byte for byte.
pub fn trim_stack_frames(mut e: Exchange) -> Exchange {
    if (200..300).contains(&e.status) {
        return e;
    }
    if let Ok(mut body) = serde_json::from_str::<serde_json::Value>(&e.response)
        && let Some(errors) = body.get_mut("errors").and_then(|v| v.as_array_mut())
    {
        for err in errors.iter_mut() {
            if let Some(s) = err.as_str() {
                let kept: Vec<&str> = s
                    .lines()
                    .filter(|l| !l.starts_with("\tat ") && !l.trim_start().starts_with("... "))
                    .collect();
                *err = serde_json::Value::String(kept.join("\n"));
            }
        }
        e.response = body.to_string();
    }
    e
}
