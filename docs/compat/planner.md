# Loams Stream: the planner and local corpus runs (preview)

Loams Stream plans Flink SQL without Flink: `loams-stream-plan` turns bound statements into a
physical plan of the engine's native operators, and `loams-stream-runtime::local` runs a plan over
bounded sources in one process. This page records what the planner lowers and what has been
measured against Apache Flink 2.2.1. Everything is **preview**.

## Pipeline

1. **Convert** (`convert.rs`): the parsed statement becomes a logical plan (scans with computed
   columns and watermarks, projections, filters, window TVFs, aggregates, joins, unions, ranks,
   sorts), typed with the binder's rules.
2. **Lower** (`lower.rs`): projections and filters merge into one `Calc`; a table's watermark is a
   `WatermarkAssigner` after its computed columns; window TVF aggregates become a `Calc` into the
   operator's `[ts, value.., key..]` layout, `WindowAggregate`, and a `Calc` back; `GROUP BY`
   becomes `GroupAggregate`; `ROW_NUMBER() OVER (...)` with a bound on the rank becomes
   `Deduplicate` (one row per key by a time attribute) or a Top-N `Rank`; equi-joins become `Join`.
3. **Encode** (`encode.rs`): expressions use the engine's prefix encoding, ported from the Java
   plugin's encoder. Loams Stream has no JVM to fall back to, so an expression the port does not
   admit fails with a named feature instead of running approximately.
4. **Changelog** (`changelog.rs`): each node's changelog mode, checked against what the sink
   accepts.

Constructs not lowered yet fail with a named feature and the phase they are planned for (D2268).

## Local corpus runs

`cargo test -p loams-stream-runtime --test corpus -- --ignored --nocapture corpus_report` plans
every recorded corpus case, runs it in-process and compares the result with the one recorded
from Flink 2.2.1, with the harness's normaliser. Restore cases read `data/` and then
`data-after-restore/` in one job, with a watermark per row (savepoints come with Task 14).

Measured on 2026-10-30 (fork PR for FC1 Task 10, part 1):

| Corpus | Match |
|---|---|
| Nexmark (q6 excluded by the oracle) | 18 of 22 |
| Flink test programs in FC1's scope | 106 of 180 |
| Smoke | 0 of 20 (the local executor has no `datagen` source yet) |

The largest remaining gaps: the `datagen` source in the local executor, `NOT NULL` sink columns
(`ConstraintEnforcer`), Java UDFs (FC2), subqueries, casts between `TIMESTAMP` and
`TIMESTAMP_LTZ`, temporal arithmetic, nested columns that are `NOT NULL` inside nullable rows,
the error-reporting cases (the oracle records the job's error), updates into keyed sinks
(`SinkMaterializer`), `FROM_UNIXTIME`, legacy group windows (Nexmark q11) and the csv format
(Nexmark q13).
