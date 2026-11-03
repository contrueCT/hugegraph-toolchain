/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hugegraph.benchmark.reference;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;

/** One-shot, fixed-schema writer. Outer cgroup/process supervision is required. */
public final class ReferenceSchemaMain {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String SCHEMA_SHA = "b49be47b68160a25e640c8f52cfa6423e7cecb50fc893a1522fe516d76792a3f";
    static final Set<String> LABELS = Set.of("City", "Comment", "Country", "Forum", "Message", "Organisation",
                                             "Person", "Post", "Tag", "Place", "TagClass");
    static final Map<String, String> INDEXES = Map.of("Country", "name", "Message", "creationDate",
            "Person", "firstName", "Post", "creationDate", "Tag", "name", "TagClass", "name");

    record Definition(String name, String label, String property, boolean unique, String cypher) { }

    interface Database extends AutoCloseable {
        Map<String, Object> inspect();
        Map<String, Object> submit(String cypher, int seconds);
        @Override
        void close();
    }

    private static void require(boolean value, String reason) {
        if (!value) {
            throw new IllegalArgumentException(reason);
        }
    }

    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void save(Path file, Object value) throws Exception {
        Files.write(file, JSON.writeValueAsBytes(value), StandardOpenOption.CREATE_NEW);
    }

    static List<Definition> definitions(byte[] raw) throws Exception {
        require(raw.length <= 65536 && sha(raw).equals(SCHEMA_SHA), "Fixed schema identity differs");
        String text = Arrays.stream(new String(raw, StandardCharsets.UTF_8).split("\\R"))
                .filter(line -> !line.stripLeading().startsWith("//")).collect(Collectors.joining("\n"));
        Pattern unique = Pattern.compile("CREATE CONSTRAINT (\\w+) FOR \\(n:(\\w+)\\) REQUIRE n\\.id IS UNIQUE");
        Pattern index = Pattern.compile("CREATE BTREE INDEX (\\w+) FOR \\(n:(\\w+)\\) ON \\(n\\.(\\w+)\\)");
        List<Definition> result = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (String rawStatement : text.split(";")) {
            String query = rawStatement.strip();
            if (query.isEmpty()) {
                continue;
            }
            var constraint = unique.matcher(query);
            var attribute = index.matcher(query);
            Definition definition;
            if (constraint.matches()) {
                String label = constraint.group(2);
                require(LABELS.contains(label) && constraint.group(1).equals("hgb_reference_" + label + "_id_unique"),
                        "Unexpected unique constraint");
                definition = new Definition(constraint.group(1), label, "id", true, query);
            } else {
                require(attribute.matches(), "Unexpected fixed schema statement");
                String label = attribute.group(2), property = attribute.group(3);
                require(property.equals(INDEXES.get(label)) &&
                        attribute.group(1).equals("hgb_reference_" + label + "_" + property), "Unexpected index");
                definition = new Definition(attribute.group(1), label, property, false, query);
            }
            require(names.add(definition.name()), "Repeated schema statement");
            result.add(definition);
        }
        require(result.size() == 17 && result.stream().filter(Definition::unique).count() == 11,
                "Complete schema requires 11 constraints and 6 indexes");
        return result;
    }

    static void identity(JsonNode observed, JsonNode plan) {
        require(observed.path("databaseInfo").equals(plan.path("databaseInfo")), "Database identity changed");
        require(observed.path("nodes").isIntegralNumber() && observed.path("nodes").asLong() == 282637871L &&
                observed.path("relationships").isIntegralNumber() &&
                observed.path("relationships").asLong() == 1775513741L, "Full SF100 counts differ");
        JsonNode settings = observed.path("settings");
        require(settings.path("dbms.read_only").asText().equals("false"),
                "Schema requires a deliberately writable Neo4j 4.4 deployment");
    }

    private static String signature(JsonNode row) {
        return row.path("labelsOrTypes").toString() + ":" + row.path("properties").toString();
    }

    static boolean ready(JsonNode observed, List<Definition> definitions, Set<String> lookupNames) {
        Map<String, Definition> constraints = new TreeMap<>(), attributes = new TreeMap<>();
        Set<String> signatures = new HashSet<>();
        for (Definition d : definitions) {
            (d.unique() ? constraints : attributes).put(d.name(), d);
            signatures.add("[\"" + d.label() + "\"]:[\"" + d.property() + "\"]");
        }
        Set<String> foundConstraints = new HashSet<>(), foundIndexes = new HashSet<>(), foundLookups = new HashSet<>();
        require(observed.path("constraints").isArray() && observed.path("indexes").isArray(), "Missing metadata");
        for (JsonNode row : observed.path("constraints")) {
            Definition d = constraints.get(row.path("name").asText());
            require(d != null && row.path("entityType").asText().equals("NODE") &&
                    Set.of("UNIQUENESS", "NODE_PROPERTY_UNIQUENESS").contains(row.path("type").asText()) &&
                    signature(row).equals("[\"" + d.label() + "\"]:[\"id\"]") &&
                    foundConstraints.add(d.name()), "Unexpected constraint metadata");
        }
        require(foundConstraints.equals(constraints.keySet()), "Missing fixed constraints");
        boolean online = true;
        for (JsonNode row : observed.path("indexes")) {
            String name = row.path("name").asText(), state = row.path("state").asText();
            require(Set.of("ONLINE", "POPULATING").contains(state), "Index failed or has unknown state");
            online &= state.equals("ONLINE");
            if (row.path("type").asText().equals("LOOKUP")) {
                require(lookupNames.contains(name) && foundLookups.add(name), "Unexpected lookup index");
            } else {
                String shape = signature(row);
                require(row.path("type").asText().equals("BTREE") && row.path("entityType").asText().equals("NODE") &&
                        signatures.contains(shape) && foundIndexes.add(shape), "Unexpected or repeated index");
                Definition d = attributes.containsKey(name) ? attributes.get(name) : constraints.get(name);
                require(d != null && shape.equals("[\"" + d.label() + "\"]:[\"" + d.property() + "\"]"),
                        "Named index property differs");
            }
        }
        require(foundIndexes.equals(signatures) && foundLookups.equals(lookupNames), "Missing fixed indexes");
        return online;
    }

    static void run(JsonNode plan, byte[] schema, Path output, Supplier<Database> factory) throws Exception {
        require(plan.path("schemaVersion").asInt() == 1 &&
                plan.path("kind").asText().equals("reference-schema-plan") &&
                plan.path("scale").asText().equals("100") && plan.path("database").asText().equals("neo4j"),
                "Expected complete SF100 schema plan");
        JsonNode window = plan.path("writerWindow"), budget = plan.path("budget");
        require(plan.path("importPlanDigest").asText().matches("[a-f0-9]{64}") &&
                plan.path("importReceiptSha256").asText().matches("[a-f0-9]{64}") &&
                plan.path("databaseInfo").path("name").asText().equals("neo4j") &&
                !plan.path("databaseInfo").path("id").asText().isEmpty(), "Missing import or database binding");
        int seconds = budget.path("maxSeconds").asInt();
        require(budget.path("maxSeconds").isIntegralNumber() && budget.path("querySeconds").isIntegralNumber() &&
                seconds > 0 && seconds <= 86400 && budget.path("querySeconds").asInt() > 0 &&
                budget.path("querySeconds").asInt() <= 3600 && budget.path("reserveFreeBytes").isIntegralNumber() &&
                budget.path("reserveFreeBytes").asLong() >= 0, "Invalid schema budget");
        require(window.path("kind").asText().equals("reference-schema-writer-window") &&
                window.path("databaseInfo").equals(plan.path("databaseInfo")) &&
                window.path("importPlanDigest").equals(plan.path("importPlanDigest")) &&
                window.path("exclusiveSchemaWriter").isBoolean() && window.path("exclusiveSchemaWriter").asBoolean() &&
                window.path("otherWritersStopped").isBoolean() && window.path("otherWritersStopped").asBoolean() &&
                window.path("inflightWritesDrained").isBoolean() && window.path("inflightWritesDrained").asBoolean(),
                "Missing exclusive schema writer window");
        Instant expires = Instant.parse(window.path("validUntil").asText());
        require(expires.isAfter(Instant.now().plusSeconds(seconds)), "Writer window does not span execution");
        List<Definition> queries = definitions(schema);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        Files.createDirectory(output);
        save(output.resolve("plan.json"), plan);
        Path steps = Files.createDirectory(output.resolve("statements"));
        int acknowledged = 0;
        boolean pending = false;
        try (Database database = factory.get()) {
            guard(output, budget, deadline, expires);
            JsonNode before = JSON.valueToTree(database.inspect());
            guard(output, budget, deadline, expires);
            identity(before, plan);
            require(before.path("constraints").isArray() && before.path("constraints").isEmpty() &&
                    before.path("indexes").isArray(), "Schema already present; no automatic replay");
            Set<String> lookups = new HashSet<>();
            for (JsonNode row : before.path("indexes")) {
                require(row.path("type").asText().equals("LOOKUP") && row.path("state").asText().equals("ONLINE") &&
                        lookups.add(row.path("name").asText()), "Fresh import has unexpected indexes");
            }
            save(output.resolve("before.json"), before);
            for (Definition query : queries) {
                guard(output, budget, deadline, expires);
                String prefix = String.format("%02d", acknowledged + 1);
                save(steps.resolve(prefix + "-intent.json"), Map.of("name", query.name(), "cypher", query.cypher(),
                        "schemaSha256", SCHEMA_SHA, "at", Instant.now().toString()));
                pending = true;
                int remaining = (int) Math.max(1, TimeUnit.NANOSECONDS.toSeconds(deadline - System.nanoTime()));
                Map<String, Object> counters = database.submit(query.cypher(),
                        Math.min(remaining, budget.path("querySeconds").asInt()));
                save(steps.resolve(prefix + "-acknowledged.json"), counters);
                pending = false;
                acknowledged++;
            }
            JsonNode after;
            do {
                guard(output, budget, deadline, expires);
                after = JSON.valueToTree(database.inspect());
                guard(output, budget, deadline, expires);
                identity(after, plan);
                if (ready(after, queries, lookups)) {
                    break;
                }
                Thread.sleep(Math.min(5000, Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline-System.nanoTime()))));
            } while (true);
            save(output.resolve("after.json"), after);
            save(output.resolve("result.json"), Map.of("kind", "reference-schema-result", "schemaSha256", SCHEMA_SHA,
                    "statementsAcknowledged", acknowledged, "allIndexesOnline", true, "schemaApplied", true,
                    "requiresReadOnlyRestart", true, "dataCorrectnessAccepted", false, "performanceAccepted", false));
        } catch (Exception error) {
            save(output.resolve("failure.json"), Map.of("error", error.getClass().getName(),
                    "message", String.valueOf(error.getMessage()), "statementsAcknowledged", acknowledged,
                    "lastWriteOutcome", pending ? "unknown" : "no-pending-statement", "automaticReplay", false));
            throw error;
        }
    }

    private static void guard(Path output, JsonNode budget, long deadline, Instant expires) throws Exception {
        require(!Thread.currentThread().isInterrupted() && System.nanoTime() < deadline && Instant.now().isBefore(expires),
                "Schema deadline, cancellation or writer-window expiry");
        require(Files.getFileStore(output).getUsableSpace() >= budget.path("reserveFreeBytes").asLong(),
                "Schema free-space reserve reached");
    }

    private static Database connect(JsonNode plan) {
        URI uri = URI.create(plan.path("uri").asText());
        require(uri.getScheme().equals("bolt") && Set.of("127.0.0.1", "localhost", "[::1]").contains(uri.getHost()) &&
                uri.getPort() > 0 && uri.getUserInfo() == null && uri.getQuery() == null && uri.getFragment() == null &&
                (uri.getPath() == null || uri.getPath().isEmpty()), "Use an isolated loopback Bolt URI");
        String user = System.getenv("HGBENCH_NEO4J_USER");
        var auth = user == null ? AuthTokens.none() : AuthTokens.basic(user,
                java.util.Objects.requireNonNull(System.getenv("HGBENCH_NEO4J_PASSWORD")));
        var driver = GraphDatabase.driver(uri, auth, Config.builder().withoutEncryption().withMaxConnectionPoolSize(1)
                .withConnectionTimeout(5, TimeUnit.SECONDS).withConnectionAcquisitionTimeout(10, TimeUnit.SECONDS)
                .withMaxTransactionRetryTime(0, TimeUnit.SECONDS).build());
        var session = driver.session(SessionConfig.builder().withDatabase("neo4j")
                .withDefaultAccessMode(AccessMode.WRITE).build());
        return new Database() {
            @Override
            public Map<String, Object> inspect() {
                return ReferenceGraphVerifier.inspect(session);
            }

            @Override
            public Map<String, Object> submit(String query, int seconds) {
                var result = session.run(query, TransactionConfig.builder().withTimeout(Duration.ofSeconds(seconds))
                        .build());
                var counters = result.consume().counters();
                require(counters.nodesCreated() == 0 && counters.nodesDeleted() == 0 &&
                        counters.relationshipsCreated() == 0 && counters.relationshipsDeleted() == 0 &&
                        counters.propertiesSet() == 0 && counters.labelsAdded() == 0 && counters.labelsRemoved() == 0,
                        "Schema statement reported graph-data changes");
                return Map.of("constraintsAdded", counters.constraintsAdded(), "indexesAdded", counters.indexesAdded(),
                              "containsUpdates", counters.containsUpdates(), "at", Instant.now().toString());
            }

            @Override
            public void close() {
                try {
                    session.close();
                } finally {
                    driver.close();
                }
            }
        };
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 6 && args[0].equals("--plan") && args[2].equals("--plan-sha256") &&
                args[4].equals("--out"), "Use --plan FILE --plan-sha256 SHA256 --out NEW_DIRECTORY");
        byte[] raw;
        try (var input = Files.newInputStream(Path.of(args[1]))) {
            raw = input.readNBytes(8 * 1024 * 1024 + 1);
        }
        require(raw.length <= 8 * 1024 * 1024 && sha(raw).equals(args[3]), "Schema plan identity differs");
        JsonNode plan = JSON.readTree(raw);
        Path source = Path.of(plan.path("schemaFile").asText());
        require(Files.size(source) <= 65536, "Schema source exceeds its fixed size budget");
        byte[] schema = Files.readAllBytes(source);
        run(plan, schema, Path.of(args[5]), () -> connect(plan));
    }
}
