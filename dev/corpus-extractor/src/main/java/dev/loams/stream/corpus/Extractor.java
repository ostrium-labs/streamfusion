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

import java.nio.file.Path;

/**
 * The corpus extractor's entry point (preview).
 *
 * <pre>
 * corpus-extractor list                 print what reflection finds
 * corpus-extractor export &lt;dir&gt;         write every program as a case under dir, plus manifest.json
 * corpus-extractor verify &lt;dir&gt;         compare each case's recorded expected.jsonl with Flink's own
 *                                       expectation (flink-expected.txt); writes verify.json
 * </pre>
 */
public final class Extractor {

    private Extractor() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "list":
                {
                    Discovery d = Discovery.run();
                    for (var e : d.programs.entrySet()) {
                        var f = e.getValue();
                        StringBuilder sb = new StringBuilder(e.getKey());
                        sb.append(f.isRestore() ? "  restore" : "  plain");
                        for (var o : f.origins) {
                            sb.append("  ")
                                    .append(o.className.substring(Discovery.PACKAGE.length() + 1))
                                    .append('.')
                                    .append(o.member);
                        }
                        System.out.println(sb);
                    }
                    for (var u : d.unloadable) {
                        System.out.println("UNLOADABLE " + u.where + ": " + u.reason);
                    }
                    System.out.printf(
                            "%d classes, %d references, %d programs, %d unloadable%n",
                            d.classes.size(), d.references, d.programs.size(), d.unloadable.size());
                    break;
                }
            case "export":
                {
                    need(args, 2);
                    Discovery d = Discovery.run();
                    new CaseWriter(Path.of(args[1])).writeAll(d);
                    break;
                }
            case "verify":
                {
                    need(args, 2);
                    System.exit(Verify.run(Path.of(args[1])) ? 0 : 1);
                    break;
                }
            default:
                usage();
                System.exit(2);
        }
        System.exit(0);
    }

    private static void need(String[] args, int n) {
        if (args.length < n) {
            usage();
            System.exit(2);
        }
    }

    private static void usage() {
        System.err.println("usage: corpus-extractor <list | export DIR | verify DIR>");
    }
}
