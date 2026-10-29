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

import org.apache.flink.table.test.program.TableTestProgram;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Finds every {@link TableTestProgram} the stream exec-node test classes hold, by reflection
 * over the flink-table-planner test jar.
 *
 * <ul>
 *   <li>{@code *TestPrograms}: every static field of type {@code TableTestProgram} (and every
 *       static field holding a collection of them).
 *   <li>{@code *RestoreTest} and {@code *SemanticTests}: an instance's {@code programs()}. A
 *       restore test's programs are restore programs, unless the test runs them with {@code
 *       AfterRestoreSource.NO_RESTORE}.
 * </ul>
 */
final class Discovery {

    static final String PACKAGE = "org.apache.flink.table.planner.plan.nodes.exec.stream";

    /** Where a program was found. */
    static final class Origin {
        final String className;
        final String kind; // restore | semantic | programs
        final String member; // field name, or programs()
        final String afterRestoreSource; // FINITE | INFINITE | NO_RESTORE, restore tests only

        Origin(String className, String kind, String member, String afterRestoreSource) {
            this.className = className;
            this.kind = kind;
            this.member = member;
            this.afterRestoreSource = afterRestoreSource;
        }
    }

    /** A program and every place it was found. */
    static final class Found {
        final TableTestProgram program;
        final List<Origin> origins = new ArrayList<>();

        Found(TableTestProgram program) {
            this.program = program;
        }

        /**
         * A restore case: run by a restore test that restores, or held only by a {@code
         * *TestPrograms} class while carrying data for after the restore.
         */
        boolean isRestore() {
            boolean restoring =
                    origins.stream()
                            .anyMatch(
                                    o ->
                                            o.kind.equals("restore")
                                                    && !"NO_RESTORE".equals(o.afterRestoreSource));
            boolean runnerless = origins.stream().allMatch(o -> o.kind.equals("programs"));
            boolean afterData =
                    program.getSetupSourceTestSteps().stream()
                            .anyMatch(s -> !s.dataAfterRestore.isEmpty());
            return restoring || (runnerless && afterData);
        }

        String afterRestoreSource() {
            return origins.stream()
                    .filter(o -> o.kind.equals("restore") && o.afterRestoreSource != null)
                    .map(o -> o.afterRestoreSource)
                    .findFirst()
                    .orElse("FINITE");
        }
    }

    /** A class or member that could not be loaded, and why. */
    static final class Unloadable {
        final String where;
        final String reason;

        Unloadable(String where, String reason) {
            this.where = where;
            this.reason = reason;
        }
    }

    final List<String> classes = new ArrayList<>();
    /** Keyed by program id, in discovery order. Same id, different object: kept apart. */
    final Map<String, Found> programs = new LinkedHashMap<>();

    final List<Unloadable> unloadable = new ArrayList<>();

    /** Every (class, member) → program reference reflection saw, before de-duplication. */
    int references;

    static Discovery run() throws IOException {
        Discovery d = new Discovery();
        for (String cls : d.classNames()) {
            d.classes.add(cls);
            d.visit(cls);
        }
        return d;
    }

    private List<String> classNames() throws IOException {
        Path jar;
        try {
            jar =
                    Path.of(
                            Class.forName(
                                            "org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase")
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
        } catch (ClassNotFoundException | URISyntaxException e) {
            throw new IOException("the flink-table-planner test jar is not on the classpath", e);
        }
        String prefix = PACKAGE.replace('.', '/') + "/";
        TreeSet<String> names = new TreeSet<>();
        try (JarFile jf = new JarFile(jar.toFile())) {
            Enumeration<JarEntry> en = jf.entries();
            while (en.hasMoreElements()) {
                String n = en.nextElement().getName();
                if (!n.startsWith(prefix) || !n.endsWith(".class")) {
                    continue;
                }
                String simple = n.substring(prefix.length(), n.length() - ".class".length());
                if (simple.contains("/") || simple.contains("$")) {
                    continue;
                }
                if (simple.endsWith("SemanticTests")
                        || simple.endsWith("TestPrograms")
                        || simple.endsWith("RestoreTest")) {
                    names.add(PACKAGE + "." + simple);
                }
            }
        }
        return new ArrayList<>(names);
    }

    private void visit(String className) {
        Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (Throwable e) {
            unloadable.add(new Unloadable(className, "class does not load: " + describe(e)));
            return;
        }
        String simple = cls.getSimpleName();
        if (simple.endsWith("TestPrograms")) {
            for (Field f : cls.getDeclaredFields()) {
                if (!Modifier.isStatic(f.getModifiers())) {
                    continue;
                }
                Object v;
                try {
                    f.setAccessible(true);
                    v = f.get(null);
                } catch (Throwable e) {
                    if (TableTestProgram.class.isAssignableFrom(f.getType())) {
                        unloadable.add(
                                new Unloadable(
                                        className + "." + f.getName(),
                                        "field does not load: " + describe(e)));
                    }
                    continue;
                }
                if (v instanceof TableTestProgram) {
                    add((TableTestProgram) v, new Origin(className, "programs", f.getName(), null));
                } else if (v instanceof Collection) {
                    for (Object o : (Collection<?>) v) {
                        if (o instanceof TableTestProgram) {
                            add(
                                    (TableTestProgram) o,
                                    new Origin(className, "programs", f.getName(), null));
                        }
                    }
                }
            }
            return;
        }
        if (Modifier.isAbstract(cls.getModifiers())) {
            return;
        }
        String kind = simple.endsWith("RestoreTest") ? "restore" : "semantic";
        Object instance;
        try {
            var ctor = cls.getDeclaredConstructor();
            ctor.setAccessible(true);
            instance = ctor.newInstance();
        } catch (Throwable e) {
            unloadable.add(new Unloadable(className, "cannot instantiate: " + describe(e)));
            return;
        }
        String after = null;
        if (kind.equals("restore")) {
            after = afterRestoreSource(instance);
        }
        List<?> list;
        try {
            Method m = findMethod(cls, "programs");
            m.setAccessible(true);
            list = (List<?>) m.invoke(instance);
        } catch (Throwable e) {
            unloadable.add(new Unloadable(className + ".programs()", describe(e)));
            return;
        }
        for (Object o : list) {
            add((TableTestProgram) o, new Origin(className, kind, "programs()", after));
        }
    }

    private void add(TableTestProgram p, Origin origin) {
        references++;
        Found f = programs.get(p.id);
        if (f == null) {
            f = new Found(p);
            programs.put(p.id, f);
        } else if (f.program != p) {
            // Two different programs share an id: keep the second under a suffixed key.
            String key = p.id;
            int n = 2;
            while (programs.containsKey(key) && programs.get(key).program != p) {
                key = p.id + "~" + n++;
            }
            f = programs.computeIfAbsent(key, k -> new Found(p));
        }
        for (Origin o : f.origins) {
            if (o.className.equals(origin.className) && o.member.equals(origin.member)) {
                return;
            }
        }
        f.origins.add(origin);
    }

    private static String afterRestoreSource(Object restoreTest) {
        Class<?> c = restoreTest.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField("afterRestoreSource");
                f.setAccessible(true);
                Object v = f.get(restoreTest);
                return v == null ? null : v.toString();
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        return null;
    }

    private static Method findMethod(Class<?> c, String name) throws NoSuchMethodException {
        Class<?> k = c;
        while (k != null) {
            try {
                return k.getDeclaredMethod(name);
            } catch (NoSuchMethodException e) {
                k = k.getSuperclass();
            }
        }
        return c.getMethod(name);
    }

    static String describe(Throwable e) {
        Throwable t = e;
        while ((t instanceof java.lang.reflect.InvocationTargetException
                        || t instanceof ExceptionInInitializerError)
                && t.getCause() != null) {
            t = t.getCause();
        }
        String m = t.getMessage();
        if (m != null) {
            m = m.replace('\n', ' ');
            if (m.length() > 300) {
                m = m.substring(0, 300) + "…";
            }
        }
        return t.getClass().getName() + (m == null ? "" : ": " + m);
    }
}
