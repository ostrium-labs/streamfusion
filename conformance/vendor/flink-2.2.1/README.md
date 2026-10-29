# Apache Flink 2.2.1 files (vendored)

Files copied unchanged from [apache/flink](https://github.com/apache/flink) at
tag `release-2.2.1` (commit `450c63e961805a90aa18cb815d6b412f44303b9b`),
licensed under the Apache License 2.0 (Copyright The Apache Software
Foundation). Design 82 §18 allows vendoring them with attribution; Loams
Stream's conformance tests read them.

| File | Source path |
|---|---|
| `openapi/rest_v1_sql_gateway.yml` | `docs/static/generated/rest_v1_sql_gateway.yml` |
| `openapi/rest_v2_sql_gateway.yml` | `docs/static/generated/rest_v2_sql_gateway.yml` |
| `openapi/rest_v3_sql_gateway.yml` | `docs/static/generated/rest_v3_sql_gateway.yml` |
| `openapi/rest_v4_sql_gateway.yml` | `docs/static/generated/rest_v4_sql_gateway.yml` |

Note: the generated files name the result flag `queryResult`; the gateway
sends `isQueryResult` (the recorded fixtures in
`native/loams-stream-conformance/tests/fixtures/gateway/` show the wire form).
