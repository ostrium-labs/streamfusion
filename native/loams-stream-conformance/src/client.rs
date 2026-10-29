//! A client for the Flink SQL Gateway REST API, v1 to v4.
//!
//! Target-agnostic: it speaks the API as Flink 2.2.1 documents it
//! (`conformance/vendor/flink-2.2.1/openapi/`), so the same code drives
//! Apache Flink and, later, Loams Stream. Ported from `ostrium-labs/loams`
//! `crates/loams-flink/src/sql_gateway.rs` (Apache-2.0, same owner), with
//! result fields kept as raw JSON text and an optional exchange recorder
//! for the recorded-fixture tests.
//!
//! | Call | Endpoint | Since |
//! |---|---|---|
//! | [`GatewayClient::api_versions`] | `GET /api_versions` | v1 |
//! | [`GatewayClient::info`] | `GET /info` | v1 |
//! | [`GatewayClient::open_session`] | `POST /sessions` | v1 |
//! | [`GatewayClient::session_config`] | `GET /sessions/:h` | v1 |
//! | [`GatewayClient::heartbeat`] | `POST /sessions/:h/heartbeat` | v1 |
//! | [`GatewayClient::close_session`] | `DELETE /sessions/:h` | v1 |
//! | [`GatewayClient::configure_session`] | `POST /sessions/:h/configure-session` | v2 |
//! | [`GatewayClient::complete_statement`] | `GET /sessions/:h/complete-statement` | v2 |
//! | [`GatewayClient::execute_statement`] | `POST /sessions/:h/statements` | v1 |
//! | [`GatewayClient::operation_status`] | `GET /sessions/:h/operations/:o/status` | v1 |
//! | [`GatewayClient::cancel_operation`] | `POST /sessions/:h/operations/:o/cancel` | v1 |
//! | [`GatewayClient::close_operation`] | `DELETE /sessions/:h/operations/:o/close` | v1 |
//! | [`GatewayClient::fetch_result`] | `GET /sessions/:h/operations/:o/result/:token` | v1 (`rowFormat` v2) |
//! | [`GatewayClient::deploy_script`] | `POST /sessions/:h/scripts` | v4 |
//!
//! `STOP JOB '<id>' [WITH SAVEPOINT] [WITH DRAIN]` is a statement:
//! [`GatewayClient::stop_job`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::{Method, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::error::{Error, Result};
use crate::result::{Column, ResultTable, Row, RowKind, render_logical_type};

/// A SQL Gateway REST API version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ApiVersion {
    /// v1: sessions, statements, operations, results.
    V1,
    /// v2: adds `configure-session`, `complete-statement` and `rowFormat`.
    V2,
    /// v3: adds materialized-table refresh.
    V3,
    /// v4: adds `POST /sessions/:h/scripts` (application mode).
    V4,
}

impl ApiVersion {
    /// Every version, oldest first.
    pub const ALL: [ApiVersion; 4] = [
        ApiVersion::V1,
        ApiVersion::V2,
        ApiVersion::V3,
        ApiVersion::V4,
    ];

    /// The path prefix, `v1`..`v4`.
    pub fn path(self) -> &'static str {
        match self {
            ApiVersion::V1 => "v1",
            ApiVersion::V2 => "v2",
            ApiVersion::V3 => "v3",
            ApiVersion::V4 => "v4",
        }
    }

    /// Parses `V3`, `v3` or `3`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().trim_start_matches(['v', 'V']) {
            "1" => Some(ApiVersion::V1),
            "2" => Some(ApiVersion::V2),
            "3" => Some(ApiVersion::V3),
            "4" => Some(ApiVersion::V4),
            _ => None,
        }
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.path())
    }
}

fn check_segment(what: &str, value: &str) -> Result<()> {
    if !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        Ok(())
    } else {
        Err(Error::Invalid(format!("bad {what}: {value:?}")))
    }
}

macro_rules! handle {
    ($t:ident, $what:literal) => {
        #[doc = concat!("A ", $what, " the gateway issued (a UUID).")]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $t(String);

        impl $t {
            /// Checks the handle's shape (letters, digits and `-`).
            ///
            /// # Errors
            ///
            /// [`Error::Invalid`] for anything else.
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                check_segment($what, &value)?;
                Ok(Self(value))
            }

            /// The handle.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

handle!(SessionHandle, "session handle");
handle!(OperationHandle, "operation handle");

/// `GET /info`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayInfo {
    /// `Apache Flink` (or the target's product name).
    pub product_name: String,
    /// The version, e.g. `2.2.1`.
    pub version: String,
}

/// The body of `POST /sessions`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenSession {
    /// A name for the session.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_name: Option<String>,
    /// Session configuration.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

/// The body of `POST /sessions/:h/statements`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteStatement {
    /// One SQL statement.
    pub statement: String,
    /// The gateway's execution timeout, in ms.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_timeout: Option<u64>,
    /// Configuration for this statement only.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub execution_config: BTreeMap<String, String>,
}

impl ExecuteStatement {
    /// `statement` with no options.
    pub fn new(statement: impl Into<String>) -> Self {
        Self {
            statement: statement.into(),
            ..Self::default()
        }
    }

    /// Adds one execution option.
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.execution_config.insert(key.into(), value.into());
        self
    }
}

/// The body of `POST /sessions/:h/scripts` (v4).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployScript {
    /// The SQL script, inline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    /// Where the script is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_uri: Option<String>,
    /// Configuration of the application cluster.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub execution_config: BTreeMap<String, String>,
}

/// An operation's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationStatus {
    /// Created, not yet submitted.
    Initialized,
    /// Submitted.
    Pending,
    /// Running.
    Running,
    /// Done; results can be fetched.
    Finished,
    /// Cancelled.
    Canceled,
    /// Closed.
    Closed,
    /// Failed; fetching results returns the error.
    Error,
    /// Ran past its execution timeout.
    Timeout,
}

impl OperationStatus {
    /// Whether the operation can no longer change state.
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            OperationStatus::Initialized | OperationStatus::Pending | OperationStatus::Running
        )
    }
}

/// The row format a result page is fetched in (v2+).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RowFormat {
    /// Each field as JSON of its type.
    #[default]
    Json,
    /// Each field as its string form.
    PlainText,
}

impl RowFormat {
    fn as_str(self) -> &'static str {
        match self {
            RowFormat::Json => "JSON",
            RowFormat::PlainText => "PLAIN_TEXT",
        }
    }
}

/// What a result page is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResultType {
    /// Nothing yet: fetch the same token again later.
    NotReady,
    /// Rows; `nextResultUri` names the next page.
    Payload,
    /// End of stream.
    Eos,
}

/// A result column as the gateway sends it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInfo {
    /// The column name.
    pub name: String,
    /// Flink's serialized `LogicalType`.
    #[serde(default)]
    pub logical_type: Value,
}

/// A result row as the gateway sends it; fields stay raw JSON.
#[derive(Debug, Deserialize)]
pub struct RawRow {
    /// The change, `INSERT` … `DELETE`.
    pub kind: String,
    /// The fields, verbatim.
    #[serde(default)]
    pub fields: Vec<Box<RawValue>>,
}

/// The rows of a page.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultInfo {
    /// The columns (on every page).
    #[serde(default)]
    pub columns: Vec<ColumnInfo>,
    /// The rows.
    #[serde(default)]
    pub data: Vec<RawRow>,
}

/// One page of `GET .../result/:token`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultPage {
    /// `NOT_READY`, `PAYLOAD` or `EOS`.
    pub result_type: ResultType,
    /// Whether the statement is a query.
    #[serde(default)]
    pub is_query_result: Option<bool>,
    /// The job the statement submitted, if any.
    #[serde(default, rename = "jobID")]
    pub job_id: Option<String>,
    /// `SUCCESS` or `SUCCESS_WITH_CONTENT`.
    #[serde(default)]
    pub result_kind: Option<String>,
    /// The rows.
    #[serde(default)]
    pub results: ResultInfo,
    /// The next page's path, absent at `EOS`.
    #[serde(default)]
    pub next_result_uri: Option<String>,
}

impl ResultPage {
    /// The token of the next page, read from `nextResultUri`.
    pub fn next_token(&self) -> Option<u64> {
        self.next_result_uri.as_deref().and_then(token_of_uri)
    }
}

/// The last path segment of a `nextResultUri`, as a token.
pub fn token_of_uri(uri: &str) -> Option<u64> {
    let path = uri.split(['?', '#']).next().unwrap_or_default();
    path.trim_end_matches('/').rsplit('/').next()?.parse().ok()
}

/// Bounds for [`GatewayClient::fetch_all`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchLimits {
    /// Stop after this many rows (the result is then `truncated`).
    pub max_rows: usize,
    /// Give up after this long.
    pub timeout: Duration,
    /// The first wait after `NOT_READY`; doubles up to `max_poll`.
    pub poll: Duration,
    /// The longest wait between polls.
    pub max_poll: Duration,
}

impl Default for FetchLimits {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            timeout: Duration::from_secs(120),
            poll: Duration::from_millis(20),
            max_poll: Duration::from_millis(500),
        }
    }
}

/// Every page of a result, collected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Collected {
    /// The columns and rows.
    pub table: ResultTable,
    /// The job, if the statement submitted one.
    pub job_id: Option<String>,
    /// `SUCCESS` or `SUCCESS_WITH_CONTENT`.
    pub result_kind: Option<String>,
    /// Whether the statement is a query.
    pub is_query_result: Option<bool>,
    /// `max_rows` was reached before `EOS`.
    pub truncated: bool,
}

/// One request and its answer, as the recorder keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    /// `GET`, `POST` or `DELETE`.
    pub method: String,
    /// The path and query, e.g. `/v4/sessions/<h>/operations/<o>/result/0?rowFormat=JSON`.
    pub path: String,
    /// The JSON body sent, if any.
    #[serde(default)]
    pub body: Option<Value>,
    /// The HTTP status.
    pub status: u16,
    /// The response body, verbatim.
    pub response: String,
}

/// Keeps every exchange a client makes (for recording fixtures).
#[derive(Debug, Clone, Default)]
pub struct Recorder(Arc<Mutex<Vec<Exchange>>>);

impl Recorder {
    /// The exchanges so far.
    pub fn take(&self) -> Vec<Exchange> {
        std::mem::take(&mut *self.0.lock().expect("recorder lock"))
    }

    fn push(&self, e: Exchange) {
        self.0.lock().expect("recorder lock").push(e);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionHandleBody {
    session_handle: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationHandleBody {
    operation_handle: String,
}

#[derive(Deserialize)]
struct StatusBody {
    status: String,
}

#[derive(Deserialize)]
struct VersionsBody {
    #[serde(default)]
    versions: Vec<String>,
}

#[derive(Deserialize)]
struct PropertiesBody {
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct CandidatesBody {
    #[serde(default)]
    candidates: Vec<String>,
}

#[derive(Deserialize)]
struct ClusterIdBody {
    #[serde(rename = "clusterID")]
    cluster_id: String,
}

#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    errors: Vec<String>,
}

/// The SQL Gateway client. Cheap to clone.
#[derive(Debug, Clone)]
pub struct GatewayClient {
    http: reqwest::Client,
    base: Url,
    version: ApiVersion,
    recorder: Option<Recorder>,
}

impl GatewayClient {
    /// A client of the gateway at `base_url` (e.g. `http://127.0.0.1:18083`)
    /// speaking `version`.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] for a URL that is not `http://`.
    pub fn new(base_url: &str, version: ApiVersion) -> Result<Self> {
        let mut base = Url::parse(base_url)
            .map_err(|e| Error::Invalid(format!("bad gateway URL {base_url:?}: {e}")))?;
        if base.scheme() != "http" {
            return Err(Error::Invalid(format!(
                "gateway URL {base_url:?}: only http:// is supported (preview)"
            )));
        }
        if !base.path().ends_with('/') {
            let p = format!("{}/", base.path());
            base.set_path(&p);
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| Error::Invalid(format!("HTTP client: {e}")))?;
        Ok(Self {
            http,
            base,
            version,
            recorder: None,
        })
    }

    /// The same client, keeping every exchange in `recorder`.
    pub fn with_recorder(mut self, recorder: Recorder) -> Self {
        self.recorder = Some(recorder);
        self
    }

    /// The same client speaking `version`.
    pub fn with_version(mut self, version: ApiVersion) -> Self {
        self.version = version;
        self
    }

    /// The version this client speaks.
    pub fn version(&self) -> ApiVersion {
        self.version
    }

    /// The gateway's base URL.
    pub fn base_url(&self) -> &str {
        self.base.as_str()
    }

    /// Asks which versions the gateway speaks and switches to the highest.
    ///
    /// # Errors
    ///
    /// The call's error, or [`Error::Unsupported`] when none is known.
    pub async fn negotiate(mut self) -> Result<Self> {
        let offered = self.api_versions().await?;
        self.version = *offered
            .last()
            .ok_or_else(|| Error::Unsupported("the gateway offers no known API version".into()))?;
        Ok(self)
    }

    fn path(&self, rest: &str) -> String {
        format!("{}/{rest}", self.version.path())
    }

    fn require(&self, min: ApiVersion, what: &str) -> Result<()> {
        if self.version >= min {
            Ok(())
        } else {
            Err(Error::Unsupported(format!(
                "{what} needs SQL Gateway API {min} or later; this client speaks {}",
                self.version
            )))
        }
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: Option<(&str, &str)>,
        body: Option<Value>,
    ) -> Result<T> {
        let mut url = self
            .base
            .join(path)
            .map_err(|e| Error::Invalid(format!("bad path {path:?}: {e}")))?;
        if let Some((k, v)) = query {
            url.query_pairs_mut().append_pair(k, v);
        }
        let shown = url.to_string();
        let mut req = self.http.request(method.clone(), url.clone());
        if let Some(b) = &body {
            req = req
                .header("content-type", "application/json")
                .body(serde_json::to_vec(b).expect("a JSON value serializes"));
        }
        let resp = req.send().await.map_err(|e| Error::Transport {
            url: shown.clone(),
            message: e.to_string(),
        })?;
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.map_err(|e| Error::Transport {
            url: shown.clone(),
            message: e.to_string(),
        })?;
        if let Some(r) = &self.recorder {
            let mut p = url.path().to_owned();
            if let Some(q) = url.query() {
                p.push('?');
                p.push_str(q);
            }
            r.push(Exchange {
                method: method.to_string(),
                path: p,
                body: body.clone(),
                status,
                response: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        if !(200..300).contains(&status) {
            let errors = serde_json::from_slice::<ErrorBody>(&bytes)
                .map(|b| b.errors)
                .unwrap_or_else(|_| vec![String::from_utf8_lossy(&bytes).into_owned()]);
            return Err(Error::Api {
                url: shown,
                status,
                errors,
            });
        }
        let slice: &[u8] = if bytes.is_empty() { b"{}" } else { &bytes };
        serde_json::from_slice(slice).map_err(|e| Error::Decode {
            url: shown,
            message: e.to_string(),
        })
    }

    /// `GET /api_versions`: the versions offered, oldest first (unknown
    /// ones are skipped).
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn api_versions(&self) -> Result<Vec<ApiVersion>> {
        let body: VersionsBody = self.call(Method::GET, "api_versions", None, None).await?;
        let mut v: Vec<ApiVersion> = body
            .versions
            .iter()
            .filter_map(|s| ApiVersion::parse(s))
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }

    /// `GET /info`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn info(&self) -> Result<GatewayInfo> {
        self.call(Method::GET, &self.path("info"), None, None).await
    }

    /// `POST /sessions`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn open_session(&self, req: &OpenSession) -> Result<SessionHandle> {
        let body: SessionHandleBody = self
            .call(
                Method::POST,
                &self.path("sessions"),
                None,
                Some(to_value(req)),
            )
            .await?;
        SessionHandle::new(body.session_handle)
    }

    /// `GET /sessions/:h`: the session's configuration.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn session_config(&self, s: &SessionHandle) -> Result<BTreeMap<String, String>> {
        let body: PropertiesBody = self
            .call(
                Method::GET,
                &self.path(&format!("sessions/{s}")),
                None,
                None,
            )
            .await?;
        Ok(body.properties)
    }

    /// `POST /sessions/:h/heartbeat`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn heartbeat(&self, s: &SessionHandle) -> Result<()> {
        let _: Value = self
            .call(
                Method::POST,
                &self.path(&format!("sessions/{s}/heartbeat")),
                None,
                Some(serde_json::json!({})),
            )
            .await?;
        Ok(())
    }

    /// `DELETE /sessions/:h`; returns the status (`CLOSED`).
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn close_session(&self, s: &SessionHandle) -> Result<String> {
        let body: StatusBody = self
            .call(
                Method::DELETE,
                &self.path(&format!("sessions/{s}")),
                None,
                None,
            )
            .await?;
        Ok(body.status)
    }

    /// `POST /sessions/:h/configure-session` (v2+).
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] below v2, else the call's error.
    pub async fn configure_session(&self, s: &SessionHandle, statement: &str) -> Result<()> {
        self.require(ApiVersion::V2, "configure-session")?;
        let _: Value = self
            .call(
                Method::POST,
                &self.path(&format!("sessions/{s}/configure-session")),
                None,
                Some(serde_json::json!({ "statement": statement })),
            )
            .await?;
        Ok(())
    }

    /// `GET /sessions/:h/complete-statement` (v2+); Flink takes the
    /// arguments as a JSON body on the GET.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] below v2, else the call's error.
    pub async fn complete_statement(
        &self,
        s: &SessionHandle,
        statement: &str,
        position: u32,
    ) -> Result<Vec<String>> {
        self.require(ApiVersion::V2, "complete-statement")?;
        let body: CandidatesBody = self
            .call(
                Method::GET,
                &self.path(&format!("sessions/{s}/complete-statement")),
                None,
                Some(serde_json::json!({ "statement": statement, "position": position })),
            )
            .await?;
        Ok(body.candidates)
    }

    /// `POST /sessions/:h/statements`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn execute_statement(
        &self,
        s: &SessionHandle,
        req: &ExecuteStatement,
    ) -> Result<OperationHandle> {
        let body: OperationHandleBody = self
            .call(
                Method::POST,
                &self.path(&format!("sessions/{s}/statements")),
                None,
                Some(to_value(req)),
            )
            .await?;
        OperationHandle::new(body.operation_handle)
    }

    fn op_path(&self, s: &SessionHandle, o: &OperationHandle, rest: &str) -> String {
        self.path(&format!("sessions/{s}/operations/{o}/{rest}"))
    }

    /// `GET /sessions/:h/operations/:o/status`.
    ///
    /// # Errors
    ///
    /// The call's error, or [`Error::Decode`] for an unknown status.
    pub async fn operation_status(
        &self,
        s: &SessionHandle,
        o: &OperationHandle,
    ) -> Result<OperationStatus> {
        let path = self.op_path(s, o, "status");
        let body: StatusBody = self.call(Method::GET, &path, None, None).await?;
        parse_status(&path, &body.status)
    }

    /// `POST /sessions/:h/operations/:o/cancel`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn cancel_operation(
        &self,
        s: &SessionHandle,
        o: &OperationHandle,
    ) -> Result<OperationStatus> {
        let path = self.op_path(s, o, "cancel");
        let body: StatusBody = self
            .call(Method::POST, &path, None, Some(serde_json::json!({})))
            .await?;
        parse_status(&path, &body.status)
    }

    /// `DELETE /sessions/:h/operations/:o/close`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn close_operation(
        &self,
        s: &SessionHandle,
        o: &OperationHandle,
    ) -> Result<OperationStatus> {
        let path = self.op_path(s, o, "close");
        let body: StatusBody = self.call(Method::DELETE, &path, None, None).await?;
        parse_status(&path, &body.status)
    }

    /// `GET /sessions/:h/operations/:o/result/:token`, with
    /// `rowFormat=JSON` from v2 on (v1 has no `rowFormat`; its rows are JSON).
    ///
    /// # Errors
    ///
    /// The call's error; a failed statement answers 500 with its exception.
    pub async fn fetch_result(
        &self,
        s: &SessionHandle,
        o: &OperationHandle,
        token: u64,
        format: RowFormat,
    ) -> Result<ResultPage> {
        let path = self.op_path(s, o, &format!("result/{token}"));
        let query = (self.version >= ApiVersion::V2).then_some(("rowFormat", format.as_str()));
        self.call(Method::GET, &path, query, None).await
    }

    /// Fetches pages from token 0 until `EOS`, `limits.max_rows` or
    /// `limits.timeout`, polling with backoff on `NOT_READY`.
    ///
    /// # Errors
    ///
    /// The first failing fetch, [`Error::Decode`] for an unknown row kind,
    /// or [`Error::Timeout`].
    pub async fn fetch_all(
        &self,
        s: &SessionHandle,
        o: &OperationHandle,
        limits: FetchLimits,
    ) -> Result<Collected> {
        let deadline = Instant::now() + limits.timeout;
        let mut out = Collected::default();
        let mut token = 0u64;
        let mut wait = limits.poll;
        loop {
            let page = self.fetch_result(s, o, token, RowFormat::Json).await?;
            if out.table.columns.is_empty() && !page.results.columns.is_empty() {
                out.table.columns = page
                    .results
                    .columns
                    .iter()
                    .map(|c| Column::new(c.name.clone(), render_logical_type(&c.logical_type)))
                    .collect();
            }
            if page.job_id.is_some() {
                out.job_id.clone_from(&page.job_id);
            }
            if page.result_kind.is_some() {
                out.result_kind.clone_from(&page.result_kind);
            }
            if page.is_query_result.is_some() {
                out.is_query_result = page.is_query_result;
            }
            let next = page.next_token();
            match page.result_type {
                ResultType::Eos => return Ok(out),
                ResultType::NotReady => {
                    if Instant::now() + wait > deadline {
                        return Err(Error::Timeout(format!(
                            "results of operation {o} not ready after {:?}",
                            limits.timeout
                        )));
                    }
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(limits.max_poll);
                }
                ResultType::Payload => {
                    wait = limits.poll;
                    for raw in page.results.data {
                        if out.table.rows.len() >= limits.max_rows {
                            out.truncated = true;
                            return Ok(out);
                        }
                        let kind = RowKind::parse(&raw.kind).ok_or_else(|| Error::Decode {
                            url: o.to_string(),
                            message: format!("unknown row kind {:?}", raw.kind),
                        })?;
                        out.table
                            .rows
                            .push(Row::new(kind, raw.fields.iter().map(|f| f.get())));
                    }
                    if Instant::now() > deadline {
                        return Err(Error::Timeout(format!(
                            "results of operation {o} still streaming after {:?}",
                            limits.timeout
                        )));
                    }
                }
            }
            match next {
                Some(n) => token = n,
                None => return Ok(out),
            }
        }
    }

    /// Submits `req`, collects its results and closes the operation.
    ///
    /// # Errors
    ///
    /// As [`GatewayClient::execute_statement`] and [`GatewayClient::fetch_all`].
    pub async fn execute_and_collect(
        &self,
        s: &SessionHandle,
        req: &ExecuteStatement,
        limits: FetchLimits,
    ) -> Result<Collected> {
        let o = self.execute_statement(s, req).await?;
        let result = self.fetch_all(s, &o, limits).await;
        // Best effort: closing the session frees it anyway.
        let _ = self.close_operation(s, &o).await;
        result
    }

    /// `STOP JOB '<job>'`, optionally `WITH SAVEPOINT` and `WITH DRAIN`.
    /// Returns the savepoint path when one was taken.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] for a job id that is not 32 hex digits, else the
    /// statement's error.
    pub async fn stop_job(
        &self,
        s: &SessionHandle,
        job_id: &str,
        savepoint: bool,
        drain: bool,
    ) -> Result<Option<String>> {
        if job_id.len() != 32 || !job_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::Invalid(format!("bad job id {job_id:?}")));
        }
        let mut sql = format!("STOP JOB '{job_id}'");
        if savepoint {
            sql.push_str(" WITH SAVEPOINT");
        }
        if drain {
            sql.push_str(" WITH DRAIN");
        }
        let got = self
            .execute_and_collect(s, &ExecuteStatement::new(sql), FetchLimits::default())
            .await?;
        if !savepoint {
            return Ok(None);
        }
        let path = got
            .table
            .rows
            .first()
            .and_then(|r| r.fields.first())
            .and_then(|f| serde_json::from_str::<String>(f).ok());
        Ok(path)
    }

    /// `POST /sessions/:h/scripts` (v4): deploys a script as an application
    /// cluster and returns its cluster id.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] below v4, [`Error::Invalid`] unless exactly
    /// one of `script` and `script_uri` is set, else the call's error.
    pub async fn deploy_script(&self, s: &SessionHandle, req: &DeployScript) -> Result<String> {
        self.require(ApiVersion::V4, "deploy-script")?;
        if req.script.is_some() == req.script_uri.is_some() {
            return Err(Error::Invalid(
                "deploy-script takes exactly one of script and scriptUri".into(),
            ));
        }
        let body: ClusterIdBody = self
            .call(
                Method::POST,
                &self.path(&format!("sessions/{s}/scripts")),
                None,
                Some(to_value(req)),
            )
            .await?;
        Ok(body.cluster_id)
    }
}

fn to_value(v: &impl Serialize) -> Value {
    serde_json::to_value(v).expect("a request body serializes")
}

fn parse_status(path: &str, status: &str) -> Result<OperationStatus> {
    serde_json::from_value(Value::String(status.to_ascii_uppercase())).map_err(|_| Error::Decode {
        url: path.to_owned(),
        message: format!("unknown operation status {status:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_come_from_next_result_uri() {
        assert_eq!(
            token_of_uri("/v3/sessions/a/operations/b/result/7?rowFormat=JSON"),
            Some(7)
        );
        assert_eq!(
            token_of_uri("/v1/sessions/a/operations/b/result/0"),
            Some(0)
        );
        assert_eq!(token_of_uri("/v3/sessions/a"), None);
    }

    #[test]
    fn statuses_parse_case_insensitively() {
        assert_eq!(
            parse_status("p", "finished").unwrap(),
            OperationStatus::Finished
        );
        assert!(parse_status("p", "WEIRD").is_err());
        assert!(OperationStatus::Error.is_terminal());
        assert!(!OperationStatus::Running.is_terminal());
    }

    #[test]
    fn only_plain_http_urls() {
        assert!(GatewayClient::new("ftp://x", ApiVersion::V4).is_err());
        let c = GatewayClient::new("http://127.0.0.1:18083", ApiVersion::V4).unwrap();
        assert_eq!(c.base_url(), "http://127.0.0.1:18083/");
    }

    #[tokio::test]
    async fn version_gates_refuse_before_sending() {
        let c = GatewayClient::new("http://127.0.0.1:9", ApiVersion::V1).unwrap();
        let s = SessionHandle::new("s").unwrap();
        assert!(matches!(
            c.configure_session(&s, "SET 'a'='b'").await,
            Err(Error::Unsupported(_))
        ));
        let c = c.with_version(ApiVersion::V3);
        assert!(matches!(
            c.deploy_script(&s, &DeployScript::default()).await,
            Err(Error::Unsupported(_))
        ));
        let c = c.with_version(ApiVersion::V4);
        assert!(matches!(
            c.deploy_script(&s, &DeployScript::default()).await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            c.stop_job(&s, "nope", true, false).await,
            Err(Error::Invalid(_))
        ));
    }
}
