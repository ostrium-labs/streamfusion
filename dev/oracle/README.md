# The differential oracle (preview)

Loams Stream (design 82) is checked against **Apache Flink 2.2.1**: every
case runs on both engines through the same SQL Gateway REST API, and Flink's
result is the expected one. This directory holds the oracle's compose stack;
the harness is the crate `native/loams-stream-conformance` with the CLI
`loams-stream-oracle`. Preview: Loams Stream does not answer yet, so today
the harness runs in oracle-only mode and records expectations.

## Running it

From the repository root, with Podman (`podman compose`; set
`LOAMS_ORACLE_COMPOSE=podman-compose` to use that instead):

```sh
cd native && cargo build -p loams-stream-conformance && cd ..
ORACLE=<cargo target dir>/debug/loams-stream-oracle

$ORACLE up                       # start Flink 2.2.1 and wait for a TaskManager
$ORACLE record dev/corpus/smoke  # oracle-only: write each case's expected.jsonl
$ORACLE run dev/corpus/smoke     # run again and compare with expected.jsonl
$ORACLE down                     # stop and remove the containers and network
```

`run` takes `--target NAME=URL` (repeatable) to compare several gateways; the
reference is a case's `expected.jsonl` when it has one, else the first
target. `--api v1..v4` pins the API version (default: the highest the target
offers), `--only S` filters cases, `--keep` keeps each case's data and sink
files, `--out DIR` chooses where `triage.jsonl` and `summary.md` go (default
`dev/oracle/work/reports/<run>/`). The exit status is 0 only when every case
passed.

The stack (`compose.yaml`, `config.yaml`): the official
`flink:2.2.1-scala_2.12-java21` image pinned by digest, one JobManager, one
TaskManager (8 slots) and the SQL Gateway; the gateway on
`127.0.0.1:18083` and the JobManager's REST API on `127.0.0.1:18081`
(`ORACLE_GATEWAY_PORT`, `ORACLE_REST_PORT`), loopback only. `./work` (git
ignored) is mounted at `/oracle/work` in every container and holds case data,
sink files, checkpoints (every second, on the local filesystem) and reports.
Podman maps the caller to the image's `flink` user, so the files stay yours.

## Cases

A case is a directory (see `native/loams-stream-conformance/src/case.rs`):

| File | What |
|---|---|
| `case.toml` | `mode` (`streaming`/`batch`), `order_sensitive`, `changelog_significant`, optional `upsert_key`, `timeout_secs`, `collect` (`query`/`sink`), `sink`, `[config]` |
| `setup.sql` | DDL and source data, statements separated by `;` |
| `query.sql` | one statement: a `SELECT` (`collect = "query"`) or an `INSERT` (`collect = "sink"`) |
| `expected.jsonl` | the oracle's result: a header with column names and types, then one line per changelog row with its kind (`+I`, `-U`, `+U`, `-D`) and its fields as the engine wrote them |
| `data/` | files the case ships, at `${CASE_DIR}` |

Sources must be deterministic: `datagen` with `number-of-rows` and every
physical field `kind = 'sequence'` (Flink 2.2.1's datagen has no seed, so
random fields are refused when the case loads), `filesystem` on files under
`${CASE_DIR}`, or SQL `VALUES`. Sinks are `filesystem` under `${SINK_DIR}`
in `debezium-json`, so the changelog survives (Flink writes `-U` as `d` and
`+U` as `c`). Each case runs in its own session with UTC, parallelism 1 and
`table.dml-sync`.

## The normaliser's rules

1. Columns (count, names, types with nullability) must be equal.
2. Field text must be equal: `1.0`, `1.00`, `1E0` and `1` differ, as do
   `120.00` and `1.2E+2`.
3. Rows are sorted only when the case says `order_sensitive = false`.
4. The changelog is folded into its final table only when the case says
   `changelog_significant = false` (by `upsert_key` when given). A retraction
   of a row that is not there stays visible.
5. Row counts are never normalised away.

`order_sensitive = true` with `changelog_significant = false` is refused.
The property tests `normaliser_never_hides_rows_types_or_rowkinds` hold the
rules.

## Triage

Each run writes `triage.jsonl`, one line per compared pair: `case`,
`targets`, `verdict` (`match`, `mismatch`, `error-both`, `error-one`;
`recorded` in record mode; `no-expectation` for one target and no
`expected.jsonl`), the first differing row after normalisation, errors (the
root cause of Flink's exception) and row counts; and `summary.md`, the same
as a table.

## The smoke corpus

`dev/corpus/smoke`: 20 hand-written cases (tumble, hop and cumulate window
TVFs, a legacy group window, window Top-N, a window join, regular inner,
left and full outer joins, interval joins, Top-N, first- and last-row
deduplication, an unbounded aggregate's changelog, a changelog into a keyed
`debezium-json` sink folded by key and event by event, a CSV file in batch
mode, and `VALUES` in batch mode), recorded from Flink 2.2.1.
