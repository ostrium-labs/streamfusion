/*
 * Copyright 2026 Ostrium Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.loams.stream.corpus;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.expressions.DefaultSqlFactory;
import org.apache.flink.table.test.program.ConfigOptionTestStep;
import org.apache.flink.table.test.program.FailingSqlTestStep;
import org.apache.flink.table.test.program.FunctionTestStep;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.StatementSetTestStep;
import org.apache.flink.table.test.program.TableApiTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TestStep;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Writes a {@link TableTestProgram} as a case in the Task 5 format (native/loams-stream-conformance,
 * src/case.rs).
 *
 * <p>The programs run on Flink's test-only {@code values} connector, which holds rows in the
 * JVM. A case cannot: its sources become {@code filesystem} tables on files the case ships, and
 * its sinks {@code filesystem} tables in {@code debezium-json}. Insert-only sources are written in
 * {@code json}; sources whose changelog mode carries updates or deletes in {@code debezium-json}
 * (an update-after without its update-before, as an upsert source emits it, gets its before image
 * from the last row seen for its primary key). The rows are encoded by Flink's own JSON
 * serialisers, so the files read back as the same values. What the translation changes is listed
 * in the case's {@code [origin] deviations}.
 */
final class CaseWriter {

    /** A program that cannot become a case, and why. */
    static final class Unconvertible extends Exception {
        Unconvertible(String reason) {
            super(reason);
        }
    }

    private final Path out;

    CaseWriter(Path out) {
        this.out = out;
    }

    void writeAll(Discovery d) throws IOException {
        Files.createDirectories(out);
        // The directory is generated: drop every case directory a previous export wrote.
        try (Stream<Path> s = Files.list(out)) {
            for (Path p : s.collect(Collectors.toList())) {
                if (Files.isDirectory(p) && Files.exists(p.resolve("case.toml"))) {
                    deleteTree(p);
                }
            }
        }
        List<Map<String, Object>> entries = new ArrayList<>();
        int written = 0;
        int skipped = 0;
        for (var e : d.programs.entrySet()) {
            Discovery.Found f = e.getValue();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", e.getKey());
            entry.put("restore", f.isRestore());
            entry.put(
                    "origins",
                    f.origins.stream()
                            .map(
                                    o ->
                                            o.className.substring(Discovery.PACKAGE.length() + 1)
                                                    + "."
                                                    + o.member)
                            .collect(Collectors.toList()));
            String dir = dirName(e.getKey());
            try {
                Written w = writeCase(f, e.getKey(), out.resolve(dir));
                entry.put("cases", w.dirs);
                entry.put("deviations", w.deviations);
                written += w.dirs.size();
            } catch (Unconvertible u) {
                deleteTree(out.resolve(dir));
                entry.put("cases", List.of());
                entry.put("not_exported", u.getMessage());
                skipped++;
            } catch (Throwable t) {
                deleteTree(out.resolve(dir));
                entry.put("cases", List.of());
                entry.put("not_exported", "extractor error: " + Discovery.describe(t));
                skipped++;
            }
            entries.add(entry);
        }
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("flink", "2.2.1");
        manifest.put("source", "org.apache.flink:flink-table-planner_2.12:2.2.1:tests");
        manifest.put("package", Discovery.PACKAGE);
        manifest.put("classes", d.classes);
        manifest.put("references", d.references);
        manifest.put("programs_found", d.programs.size());
        manifest.put("cases_written", written);
        manifest.put("programs_exported", d.programs.size() - skipped);
        manifest.put("not_exported", skipped);
        List<Map<String, Object>> un = new ArrayList<>();
        for (var u : d.unloadable) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("where", u.where);
            m.put("reason", u.reason);
            un.add(m);
        }
        manifest.put("unloadable", un);
        manifest.put("programs", entries);
        Files.writeString(out.resolve("manifest.json"), Json.write(manifest) + "\n");
        System.out.printf(
                "%d classes, %d programs found, %d cases written, %d not exported, %d unloadable%n",
                d.classes.size(), d.programs.size(), written, skipped, d.unloadable.size());
    }

    static String dirName(String id) {
        return id.replace('~', '_').replaceAll("[^A-Za-z0-9._-]", "_");
    }

    // ------------------------------------------------------------------------------------------

    private static final class SourceInfo {
        String name;
        boolean changelog;
        boolean watermark;
    }

    /** The case directories a program became, and what the translation changed. */
    static final class Written {
        final List<String> dirs;
        final List<String> deviations;

        Written(List<String> dirs, List<String> deviations) {
            this.dirs = dirs;
            this.deviations = deviations;
        }
    }

    private Written writeCase(Discovery.Found f, String key, Path dir) throws Exception {
        TableTestProgram p = f.program;
        boolean restore = f.isRestore();
        boolean semantic = f.origins.stream().anyMatch(o -> o.kind.equals("semantic"));
        List<String> deviations = new ArrayList<>();
        TreeSet<String> requires = new TreeSet<>();

        for (TestStep s : p.setupSteps) {
            switch (s.getKind()) {
                case MODEL:
                    throw new Unconvertible(
                            "needs a model on Flink's test-only 'values' model provider (ML_PREDICT);"
                                    + " the distribution ships no model provider");
                case TEMPORAL_FUNCTION:
                    throw new Unconvertible(
                            "registers a Table API temporal table function, which has no SQL DDL");
                default:
                    break;
            }
        }
        for (TestStep s : p.runSteps) {
            if (s.getKind() == TestKind.FAILING_TABLE_API) {
                throw new Unconvertible(
                        "a failing Table API step: the failure is raised while the Table is built,"
                                + " so there is no SQL statement to replay");
            }
        }

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Map<String, String> config = new TreeMap<>();
        if (semantic) {
            // SemanticTestBase.applyDefaultEnvironmentOptions
            config.put("table.optimizer.non-deterministic-update.strategy", "TRY_RESOLVE");
        }
        List<String> setup = new ArrayList<>();
        List<SourceInfo> sources = new ArrayList<>();
        List<SinkTestStep> sinks = new ArrayList<>();
        Map<String, String> files = new LinkedHashMap<>(); // relative path -> content

        for (TestStep s : p.setupSteps) {
            switch (s.getKind()) {
                case CONFIG:
                    {
                        ConfigOptionTestStep<?> c = (ConfigOptionTestStep<?>) s;
                        c.apply(env);
                        config.put(c.option.key(), configValue(c));
                        break;
                    }
                case FUNCTION:
                    {
                        FunctionTestStep fn = (FunctionTestStep) s;
                        fn.apply(env);
                        String persistence = field(fn, "persistence").toString();
                        String behavior = field(fn, "behavior").toString();
                        String head;
                        if (behavior.equals("SYSTEM")) {
                            head = "CREATE TEMPORARY SYSTEM FUNCTION ";
                        } else if (persistence.equals("TEMPORARY")) {
                            head = "CREATE TEMPORARY FUNCTION ";
                        } else {
                            head = "CREATE FUNCTION ";
                        }
                        setup.add(
                                head
                                        + quoteIdent(fn.name)
                                        + " AS '"
                                        + fn.function.getName()
                                        + "' LANGUAGE JAVA");
                        requires.add("flink-test-udfs");
                        break;
                    }
                case SQL:
                    {
                        String sql = ((SqlTestStep) s).sql.trim();
                        if (sql.endsWith(";")) {
                            sql = sql.substring(0, sql.length() - 1);
                        }
                        if (!sql.toUpperCase().startsWith("INSERT")) {
                            env.executeSql(sql);
                        }
                        setup.add(sql);
                        if (sql.toUpperCase().contains("FUNCTION")) {
                            requires.add("flink-test-udfs");
                        }
                        break;
                    }
                case SOURCE_WITHOUT_DATA:
                case SOURCE_WITH_DATA:
                case SOURCE_WITH_RESTORE_DATA:
                    {
                        SourceTestStep src = (SourceTestStep) s;
                        src.apply(env);
                        SourceInfo info = writeSource(env, src, restore, files, setup, deviations);
                        sources.add(info);
                        break;
                    }
                case SINK_WITHOUT_DATA:
                case SINK_WITH_DATA:
                case SINK_WITH_RESTORE_DATA:
                    {
                        SinkTestStep snk = (SinkTestStep) s;
                        snk.apply(env);
                        setup.add(sinkDdl(snk, deviations));
                        sinks.add(snk);
                        break;
                    }
                default:
                    throw new Unconvertible("setup step " + s.getKind() + " has no case form");
            }
        }

        List<String> queries = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        for (TestStep s : p.runSteps) {
            String expectError = null;
            String q;
            switch (s.getKind()) {
                case SQL:
                    q = ((SqlTestStep) s).sql.trim();
                    break;
                case STATEMENT_SET:
                    {
                        List<String> stmts = ((StatementSetTestStep) s).statements;
                        q =
                                "EXECUTE STATEMENT SET\nBEGIN\n"
                                        + stmts.stream()
                                                .map(x -> stripSemicolon(x.trim()) + ";\n")
                                                .collect(Collectors.joining())
                                        + "END";
                        break;
                    }
                case TABLE_API:
                    {
                        TableApiTestStep t = (TableApiTestStep) s;
                        String sql;
                        try {
                            Table table = t.toTable(env);
                            sql =
                                    table.getQueryOperation()
                                            .asSerializableString(DefaultSqlFactory.INSTANCE);
                        } catch (Throwable e) {
                            throw new Unconvertible(
                                    "a Table API step that QueryOperation.asSerializableString"
                                            + " cannot write as SQL (an inline, unregistered function"
                                            + " or model): "
                                            + Discovery.describe(e));
                        }
                        q = "INSERT INTO " + quoteIdent((String) field(t, "sinkName")) + "\n" + sql;
                        deviations.add(
                                "Table API step written as SQL by QueryOperation.asSerializableString");
                        break;
                    }
                case FAILING_SQL:
                    {
                        FailingSqlTestStep fs = (FailingSqlTestStep) s;
                        q = fs.sql.trim();
                        expectError = fs.expectedErrorMessage;
                        break;
                    }
                default:
                    throw new Unconvertible("run step " + s.getKind() + " has no case form");
            }
            queries.add(stripSemicolon(q));
            errors.add(expectError);
        }
        if (queries.isEmpty()) {
            throw new Unconvertible("no run step");
        }
        if (queries.size() > 1
                && !p.runSteps.stream().allMatch(s -> s.getKind() == TestKind.FAILING_SQL)) {
            throw new Unconvertible(
                    "more than one run step that is not an expected failure; a case runs one"
                            + " statement (found "
                            + p.runSteps.size()
                            + ")");
        }

        boolean watermarks = sources.stream().anyMatch(si -> si.watermark);
        if (watermarks) {
            // The values connector emits a watermark per record in the semantic tests
            // ('scan.watermark.emit.strategy' = 'on-event'). A filesystem source cannot push
            // watermarks down; with a zero interval the WatermarkAssigner emits one per record.
            config.put("pipeline.auto-watermark-interval", "0 ms");
        }

        // ---- expectations
        Map<String, Object> flinkExpected = new LinkedHashMap<>();
        int beforeRows = 0;
        int afterRows = 0;
        for (SinkTestStep snk : sinks) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("test_changelog_data", snk.shouldTestChangelogData());
            m.put("before", snk.getExpectedBeforeRestoreAsStrings());
            m.put("after", snk.getExpectedAfterRestoreAsStrings());
            m.put("materialized", snk.getExpectedMaterializedResultsAsStrings());
            m.put("primary_key", primaryKey(env, snk.name));
            m.put("options", new TreeMap<>(snk.options));
            flinkExpected.put(snk.name, m);
            beforeRows += snk.getExpectedBeforeRestoreAsStrings().size();
            afterRows += snk.getExpectedAfterRestoreAsStrings().size();
        }

        // A variant per run step (several only when each is an expected failure), and per
        // collected sink (a statement set writing several sinks becomes a case per sink).
        List<Object[]> variants = new ArrayList<>(); // query, error, sink, dir suffix
        for (int run = 0; run < queries.size(); run++) {
            String suffix = queries.size() == 1 ? "" : "." + (run + 1);
            if (errors.get(run) != null || sinks.isEmpty()) {
                variants.add(new Object[] {queries.get(run), errors.get(run), null, suffix});
            } else if (sinks.size() == 1) {
                variants.add(new Object[] {queries.get(run), null, sinks.get(0), suffix});
            } else {
                for (SinkTestStep snk : sinks) {
                    variants.add(
                            new Object[] {queries.get(run), null, snk, suffix + "." + snk.name});
                }
            }
        }

        List<String> written = new ArrayList<>();
        for (Object[] v : variants) {
            String query = (String) v[0];
            String expectError = (String) v[1];
            SinkTestStep collected = (SinkTestStep) v[2];
            Path caseDir = dir.resolveSibling(dir.getFileName() + (String) v[3]);
            // ---- case.toml
            StringBuilder toml = new StringBuilder();
            toml.append("# Generated by dev/corpus-extractor from Apache Flink 2.2.1's")
                    .append(" TableTestProgram\n# \"")
                    .append(p.id)
                    .append("\" (Apache-2.0; see ../NOTICE). Do not edit by hand: re-export.\n");
            toml.append("description = ").append(Toml.str(p.description)).append('\n');
            toml.append("mode = \"streaming\"\n");
            boolean changelog = collected == null || collected.shouldTestChangelogData();
            toml.append("order_sensitive = false\n");
            toml.append("changelog_significant = ").append(changelog).append('\n');
            if (!changelog) {
                List<String> pk = primaryKey(env, collected.name);
                if (!pk.isEmpty()) {
                    toml.append("upsert_key = ").append(Toml.strList(pk)).append('\n');
                }
            }
            toml.append("timeout_secs = ").append(restore ? 180 : 120).append('\n');
            if (collected == null) {
                toml.append("collect = \"query\"\n");
            } else {
                toml.append("collect = \"sink\"\n");
                toml.append("sink = ").append(Toml.str(collected.name)).append('\n');
            }
            if (expectError != null) {
                toml.append("expect_error = ").append(Toml.str(expectError)).append('\n');
            }
            if (restore) {
                int before =
                        collected == null
                                ? 0
                                : collected.getExpectedBeforeRestoreAsStrings().size();
                String end = f.afterRestoreSource().equals("INFINITE") ? "cancel" : "drain";
                toml.append("restore = true\n");
                toml.append("restore_before_rows = ").append(before).append('\n');
                toml.append("restore_end = ").append(Toml.str(end)).append('\n');
            }
            if (!requires.isEmpty()) {
                toml.append("requires = ").append(Toml.strList(new ArrayList<>(requires))).append('\n');
            }
            toml.append("\n[config]\n");
            for (var c : config.entrySet()) {
                toml.append(Toml.str(c.getKey())).append(" = ").append(Toml.str(c.getValue())).append('\n');
            }
            toml.append("\n[origin]\n");
            toml.append("corpus = \"flink-programs\"\n");
            toml.append("flink = \"2.2.1\"\n");
            toml.append("program = ").append(Toml.str(p.id)).append('\n');
            toml.append("found_in = ")
                    .append(
                            Toml.strList(
                                    f.origins.stream()
                                            .map(
                                                    o ->
                                                            o.className.substring(
                                                                            Discovery.PACKAGE.length()
                                                                                    + 1)
                                                                    + "."
                                                                    + o.member)
                                            .collect(Collectors.toList())))
                    .append('\n');
            if (restore) {
                toml.append("after_restore_source = ")
                        .append(Toml.str(f.afterRestoreSource()))
                        .append('\n');
            }
            toml.append("expected_rows = { before = ")
                    .append(beforeRows)
                    .append(", after = ")
                    .append(afterRows)
                    .append(" }\n");
            toml.append("deviations = ").append(Toml.strList(deviations)).append('\n');

            Files.createDirectories(caseDir);
            Files.writeString(caseDir.resolve("case.toml"), toml.toString());
            Files.writeString(
                    caseDir.resolve("setup.sql"),
                    "-- Generated from Apache Flink 2.2.1's TableTestProgram \""
                            + p.id
                            + "\" (Apache-2.0).\n"
                            + setup.stream().map(x -> x + ";\n\n").collect(Collectors.joining()));
            Files.writeString(caseDir.resolve("query.sql"), query + ";\n");
            Files.writeString(
                    caseDir.resolve("flink-expected.json"), Json.write(flinkExpected) + "\n");
            for (var e : files.entrySet()) {
                Path file = caseDir.resolve(e.getKey());
                Files.createDirectories(file.getParent());
                Files.write(file, e.getValue().getBytes(StandardCharsets.UTF_8));
            }
            written.add(caseDir.getFileName().toString());
        }
        return new Written(written, deviations);
    }

    // ---- sources --------------------------------------------------------------------------------

    private SourceInfo writeSource(
            TableEnvironment env,
            SourceTestStep src,
            boolean restore,
            Map<String, String> files,
            List<String> setup,
            List<String> deviations)
            throws Unconvertible {
        SourceInfo info = new SourceInfo();
        info.name = src.name;
        info.watermark =
                src.schemaComponents.stream()
                        .anyMatch(c -> c.trim().toUpperCase().startsWith("WATERMARK"));
        String mode = src.options.getOrDefault("changelog-mode", "I");
        boolean allInserts =
                Stream.concat(src.dataBeforeRestore.stream(), src.dataAfterRestore.stream())
                        .allMatch(r -> r.getKind() == RowKind.INSERT);
        info.changelog = !(mode.trim().equals("I") && allInserts);

        ResolvedSchema schema = env.from(quoteIdent(src.name)).getResolvedSchema();
        DataType physical = schema.toPhysicalRowDataType();
        RowType rowType = (RowType) physical.getLogicalType();
        DataStructureConverter<Object, Object> conv = DataStructureConverters.getConverter(physical);
        conv.open(Thread.currentThread().getContextClassLoader());
        JsonRowDataSerializationSchema json =
                new JsonRowDataSerializationSchema(
                        rowType,
                        TimestampFormat.SQL,
                        JsonFormatOptions.MapNullKeyMode.FAIL,
                        "null",
                        false,
                        false);
        try {
            json.open(null);
        } catch (Exception e) {
            throw new Unconvertible("json serialiser: " + Discovery.describe(e));
        }
        for (var ft : rowType.getFields()) {
            if (hasNonStringMapKey(ft.getType())) {
                throw new Unconvertible(
                        "source "
                                + src.name
                                + ": column "
                                + ft.getName()
                                + " "
                                + ft.getType()
                                + " has a map with non-string keys, which Flink's json and"
                                + " debezium-json formats cannot carry");
            }
        }
        List<Integer> pk = new ArrayList<>();
        schema.getPrimaryKey()
                .ifPresent(
                        k -> {
                            for (String c : k.getColumns()) {
                                pk.add(rowType.getFieldNames().indexOf(c));
                            }
                        });
        Map<List<String>, String> last = new HashMap<>();
        String before =
                encode(src.dataBeforeRestore, conv, json, info.changelog, pk, last, src.name);
        String after = encode(src.dataAfterRestore, conv, json, info.changelog, pk, last, src.name);
        files.put("data/" + src.name + "/part-0.json", before);
        if (restore) {
            files.put("data-after-restore/" + src.name + "/part-1.json", after);
        } else if (!src.dataAfterRestore.isEmpty()) {
            deviations.add(
                    "source " + src.name + ": rows for after a restore are not used (no restore)");
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(quoteIdent(src.name)).append(" (\n  ");
        ddl.append(String.join(",\n  ", src.schemaComponents));
        ddl.append("\n)");
        if (!src.partitionKeys.isEmpty()) {
            deviations.add("source " + src.name + ": PARTITIONED BY dropped");
        }
        if (src.distribution != null) {
            deviations.add("source " + src.name + ": DISTRIBUTED BY dropped");
        }
        ddl.append(" WITH (\n  'connector' = 'filesystem',\n  'path' = '${CASE_DIR}/")
                .append(src.name)
                .append("',\n  'format' = '")
                .append(info.changelog ? "debezium-json" : "json")
                .append("'");
        if (restore) {
            // Unbounded, so the job is still running when the savepoint is taken, and the
            // restored job picks up the after-restore file.
            ddl.append(",\n  'source.monitor-interval' = '200ms'");
        }
        ddl.append("\n)");
        setup.add(ddl.toString());

        TreeMap<String, String> dropped = new TreeMap<>(src.options);
        dropped.remove("connector");
        if (!dropped.isEmpty()) {
            deviations.add(
                    "source "
                            + src.name
                            + ": values-connector options dropped: "
                            + dropped.entrySet().stream()
                                    .map(e -> "'" + e.getKey() + "'='" + e.getValue() + "'")
                                    .collect(Collectors.joining(", ")));
        }
        if (info.changelog && !mode.contains("UB") && mode.contains("UA")) {
            deviations.add(
                    "source "
                            + src.name
                            + ": an upsert source (changelog-mode "
                            + mode
                            + ") becomes a full debezium-json changelog (update-before images from"
                            + " the last row per key), so no ChangelogNormalize is planned");
        }
        if (info.changelog && mode.trim().equals("I")) {
            deviations.add(
                    "source "
                            + src.name
                            + ": declared insert-only but its rows carry other kinds; read as a"
                            + " debezium-json changelog");
        }
        return info;
    }

    private static String encode(
            List<Row> rows,
            DataStructureConverter<Object, Object> conv,
            JsonRowDataSerializationSchema json,
            boolean changelog,
            List<Integer> pk,
            Map<List<String>, String> last,
            String table)
            throws Unconvertible {
        StringBuilder sb = new StringBuilder();
        String pendingBefore = null;
        for (Row r : rows) {
            RowData rd;
            try {
                rd = (RowData) conv.toInternal(r);
            } catch (Throwable e) {
                throw new Unconvertible(
                        "source " + table + ": row " + r + " does not convert: " + Discovery.describe(e));
            }
            RowKind kind = r.getKind();
            if (rd instanceof GenericRowData) {
                ((GenericRowData) rd).setRowKind(RowKind.INSERT);
            }
            String image = new String(json.serialize(rd), StandardCharsets.UTF_8);
            if (!changelog) {
                sb.append(image).append('\n');
                continue;
            }
            List<String> key = keyOf(r, pk);
            switch (kind) {
                case INSERT:
                    flush(sb, pendingBefore);
                    pendingBefore = null;
                    sb.append(event(null, image, "c"));
                    last.put(key, image);
                    break;
                case UPDATE_BEFORE:
                    flush(sb, pendingBefore);
                    pendingBefore = image;
                    break;
                case UPDATE_AFTER:
                    {
                        // Without a primary key a lone update-after has no before image.
                        String b =
                                pendingBefore != null
                                        ? pendingBefore
                                        : (pk.isEmpty() ? null : last.get(key));
                        pendingBefore = null;
                        if (b == null) {
                            sb.append(event(null, image, "c"));
                        } else {
                            sb.append(event(b, image, "u"));
                        }
                        last.put(key, image);
                        break;
                    }
                case DELETE:
                    {
                        flush(sb, pendingBefore);
                        pendingBefore = null;
                        String b = image;
                        if (!pk.isEmpty() && last.containsKey(key)) {
                            // A delete by key carries only the key: use the full last image.
                            b = last.get(key);
                        }
                        sb.append(event(b, null, "d"));
                        last.remove(key);
                        break;
                    }
            }
        }
        flush(sb, pendingBefore);
        return sb.toString();
    }

    private static void flush(StringBuilder sb, String pendingBefore) {
        if (pendingBefore != null) {
            sb.append(event(pendingBefore, null, "d"));
        }
    }

    private static String event(String before, String after, String op) {
        return "{\"before\":"
                + (before == null ? "null" : before)
                + ",\"after\":"
                + (after == null ? "null" : after)
                + ",\"op\":\""
                + op
                + "\"}\n";
    }

    private static List<String> keyOf(Row r, List<Integer> pk) {
        List<String> k = new ArrayList<>();
        for (int i : pk) {
            Object v = i >= 0 && i < r.getArity() ? r.getField(i) : null;
            k.add(String.valueOf(v));
        }
        return k;
    }

    private static boolean hasNonStringMapKey(org.apache.flink.table.types.logical.LogicalType t) {
        if (t instanceof org.apache.flink.table.types.logical.MapType) {
            var k = ((org.apache.flink.table.types.logical.MapType) t).getKeyType();
            if (!(k instanceof org.apache.flink.table.types.logical.VarCharType
                    || k instanceof org.apache.flink.table.types.logical.CharType)) {
                return true;
            }
        }
        if (t instanceof org.apache.flink.table.types.logical.MultisetType) {
            var e = ((org.apache.flink.table.types.logical.MultisetType) t).getElementType();
            if (!(e instanceof org.apache.flink.table.types.logical.VarCharType
                    || e instanceof org.apache.flink.table.types.logical.CharType)) {
                return true;
            }
        }
        return t.getChildren().stream().anyMatch(CaseWriter::hasNonStringMapKey);
    }

    // ---- sinks ----------------------------------------------------------------------------------

    private static String sinkDdl(SinkTestStep snk, List<String> deviations) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(quoteIdent(snk.name)).append(" (\n  ");
        ddl.append(String.join(",\n  ", snk.schemaComponents));
        ddl.append("\n)");
        if (!snk.partitionKeys.isEmpty()) {
            deviations.add("sink " + snk.name + ": PARTITIONED BY dropped");
        }
        if (snk.distribution != null) {
            deviations.add("sink " + snk.name + ": DISTRIBUTED BY dropped");
        }
        ddl.append(" WITH (\n  'connector' = 'filesystem',\n  'path' = '${SINK_DIR}/")
                .append(snk.name)
                .append("',\n  'format' = 'debezium-json'\n)");
        TreeMap<String, String> dropped = new TreeMap<>(snk.options);
        dropped.remove("connector");
        if (!dropped.isEmpty()) {
            deviations.add(
                    "sink "
                            + snk.name
                            + ": values-connector options dropped: "
                            + dropped.entrySet().stream()
                                    .map(e -> "'" + e.getKey() + "'='" + e.getValue() + "'")
                                    .collect(Collectors.joining(", ")));
        }
        return ddl.toString();
    }

    private static List<String> primaryKey(TableEnvironment env, String table) {
        return env.from(quoteIdent(table))
                .getResolvedSchema()
                .getPrimaryKey()
                .map(k -> new ArrayList<>(k.getColumns()))
                .orElseGet(ArrayList::new);
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static <T> String configValue(ConfigOptionTestStep<T> c) {
        Configuration conf = new Configuration();
        @SuppressWarnings("unchecked")
        ConfigOption<T> opt = (ConfigOption<T>) c.option;
        conf.set(opt, c.value);
        String v = conf.toMap().get(opt.key());
        return v != null ? v : String.valueOf(c.value);
    }

    static Object field(Object o, String name) throws ReflectiveOperationException {
        Class<?> c = o.getClass();
        while (c != null) {
            try {
                var f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(o);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    static String quoteIdent(String name) {
        if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return name;
        }
        return "`" + name.replace("`", "``") + "`";
    }

    private static String stripSemicolon(String s) {
        String t = s.trim();
        while (t.endsWith(";")) {
            t = t.substring(0, t.length() - 1).trim();
        }
        return t;
    }

    static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) {
            return;
        }
        try (Stream<Path> s = Files.walk(p)) {
            for (Path x : s.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.delete(x);
            }
        }
    }
}
