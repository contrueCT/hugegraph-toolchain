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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;

/** Streaming exact CSV-to-Neo4j validation. Uses explicit, non-retrying read transactions only. */
public final class ReferenceGraphVerifier {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SETTINGS = "CALL dbms.listConfig() YIELD name,value WHERE name IN " +
            "['dbms.read_only','dbms.databases.default_to_read_only','dbms.databases.writable'] " +
            "RETURN name,value ORDER BY name";
    private static final String INDEXES = "SHOW INDEXES YIELD name,type,entityType,labelsOrTypes,properties,state " +
            "RETURN name,type,entityType,labelsOrTypes,properties,state ORDER BY name";
    private static final String CONSTRAINTS = "SHOW CONSTRAINTS YIELD name,type,entityType,labelsOrTypes,properties " +
            "RETURN name,type,entityType,labelsOrTypes,properties ORDER BY name";
    private final JsonNode plan;
    private final Path output;
    private final Instant leaseUntil;
    private final long deadline;
    private long nextSpace;

    private ReferenceGraphVerifier(JsonNode plan, Path output) {
        this.plan = plan;
        this.output = output;
        this.leaseUntil = Instant.parse(plan.path("leaseValidUntil").asText());
        this.deadline = System.nanoTime() + Duration.ofSeconds(limit(plan.path("budget"), "maxSeconds", 1, 172800))
                                                     .toNanos();
    }

    private static void require(boolean condition, String message) {
        ExactReferenceMatcher.require(condition, message);
    }

    private static long limit(JsonNode object, String name, long low, long high) {
        JsonNode value = object.get(name);
        require(value != null && value.isIntegralNumber() && value.canConvertToLong(), "Expected integer " + name);
        long number = value.asLong();
        require(number >= low && number <= high, "Out-of-range " + name);
        return number;
    }

    public static String query(ExactReferenceMatcher.Shape shape) {
        if (shape.kind().equals("nodes")) {
            String column = shape.columns().stream().filter(c -> c.startsWith("id:ID(")).findFirst().orElseThrow();
            String label = column.substring(6, column.length() - 1);
            ExactReferenceMatcher.name(label);
            return "UNWIND $rows AS row OPTIONAL MATCH (n:`" + label + "` {id:row.id}) " +
                   "RETURN row.ordinal AS ordinal,id(n) AS internal,labels(n) AS labels,properties(n) AS props";
        }
        return "UNWIND $rows AS row OPTIONAL MATCH (a:`" + shape.sourceLabel() + "` {id:row.source})" +
               "-[r:`" + shape.label() + "`]->(b:`" + shape.targetLabel() + "` {id:row.target}) " +
               "RETURN row.ordinal AS ordinal,id(r) AS internal,type(r) AS type,properties(r) AS props," +
               "a.id AS sourceId,b.id AS targetId,labels(a) AS sourceLabels,labels(b) AS targetLabels";
    }

    private void guard() throws IOException {
        require(System.nanoTime() < this.deadline && Instant.now().isBefore(this.leaseUntil),
                "Verification deadline or writer-quiescence lease expired");
        if (System.nanoTime() >= this.nextSpace) {
            this.nextSpace = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            require(Files.getFileStore(this.output).getUsableSpace() >=
                    limit(this.plan.path("budget"), "reserveFreeBytes", 0, Long.MAX_VALUE),
                    "Free-space reserve reached");
        }
    }

    private TransactionConfig transaction() {
        return TransactionConfig.builder().withTimeout(Duration.ofSeconds(
                limit(this.plan.path("budget"), "querySeconds", 1, 3600))).build();
    }

    private static List<Map<String, Object>> small(Session session, String query) {
        Result result = session.run(query, TransactionConfig.builder().withTimeout(Duration.ofSeconds(60)).build());
        List<Map<String, Object>> rows = new ArrayList<>();
        while (result.hasNext()) {
            require(rows.size() < 1024, "Unexpectedly large metadata result");
            rows.add(new TreeMap<>(result.next().asMap()));
        }
        require(!result.consume().counters().containsUpdates(), "Read-only metadata query reported updates");
        return rows;
    }

    public static Map<String, Object> inspect(Session session) {
        List<Map<String, Object>> infos = small(session,
                "CALL db.info() YIELD id,name,creationDate RETURN id,name,creationDate");
        require(infos.size() == 1, "Missing unique database identity");
        Map<String, String> settings = new TreeMap<>();
        for (Map<String, Object> row : small(session, SETTINGS)) {
            settings.put((String) row.get("name"), (String) row.get("value"));
        }
        return Map.of("databaseInfo", infos.get(0), "settings", settings, "indexes", small(session, INDEXES),
                      "constraints", small(session, CONSTRAINTS),
                      "nodes", small(session, "MATCH (n) RETURN count(n) AS n").get(0).get("n"),
                      "relationships", small(session, "MATCH ()-[r]->() RETURN count(r) AS n").get(0).get("n"));
    }

    private Map<String, Object> snapshot(Session session) throws IOException {
        guard();
        Map<String, Object> value = inspect(session);
        JsonNode observed = JSON.valueToTree(value);
        require(observed.path("databaseInfo").equals(this.plan.path("databaseInfo")),
                "Reference database identity changed");
        require(observed.path("databaseInfo").path("name").asText().equals(this.plan.path("database").asText()),
                "Reference database name differs");
        JsonNode settings = observed.path("settings");
        boolean legacy = settings.path("dbms.read_only").asText().equals("true");
        boolean defaultReadOnly = settings.path("dbms.databases.default_to_read_only").asText().equals("true");
        String writable = settings.path("dbms.databases.writable").asText("<missing>");
        require(legacy || defaultReadOnly && Set.of("", "[]").contains(writable),
                "Reference database must be configured read-only with no writable exceptions");
        require(observed.path("nodes").asLong(-1) == this.plan.path("expected").path("nodes").asLong(-2) &&
                observed.path("relationships").asLong(-1) ==
                this.plan.path("expected").path("relationships").asLong(-2),
                "Total database counts differ from the full CSV inventory");
        Set<String> unique = new HashSet<>();
        for (JsonNode constraint : observed.path("constraints")) {
            if (constraint.path("entityType").asText().equals("NODE") &&
                Set.of("UNIQUENESS", "NODE_PROPERTY_UNIQUENESS").contains(constraint.path("type").asText()) &&
                constraint.path("labelsOrTypes").size() == 1 && constraint.path("properties").size() == 1 &&
                constraint.path("properties").get(0).asText().equals("id")) {
                unique.add(constraint.path("labelsOrTypes").get(0).asText());
            }
        }
        require(unique.containsAll(Set.of("Person", "Place", "Organisation", "Tag", "TagClass", "Forum",
                                         "Post", "Comment", "Message")),
                "Required node/Message ID uniqueness constraints missing");
        for (JsonNode index : observed.path("indexes")) {
            require(index.path("state").asText().equals("ONLINE"), "Reference index is not ONLINE");
        }
        return value;
    }

    private static ExactReferenceMatcher.Shape shape(JsonNode table) {
        List<String> columns = new ArrayList<>();
        for (JsonNode column : table.path("columns")) {
            require(column.isTextual(), "Non-textual header"); columns.add(column.asText());
        }
        return ExactReferenceMatcher.Shape.create(table.path("kind").asText(), table.path("label").asText(), columns);
    }

    private static Set<String> labels(Value value) {
        require(!value.isNull(), "Missing graph labels");
        List<String> labels = value.asList(Value::asString);
        Set<String> unique = new TreeSet<>(labels);
        require(labels.size() == unique.size(), "Repeated graph labels");
        return unique;
    }

    public static ExactReferenceMatcher.Observed observed(Record row, boolean nodes) {
        if (row.get("internal").isNull()) { return null; }
        long id = row.get("internal").asLong();
        Map<String, Object> props = ExactReferenceMatcher.properties(row.get("props"));
        if (nodes) {
            return new ExactReferenceMatcher.Observed(id, labels(row.get("labels")), props,
                                                      -1, -1, Set.of(), Set.of());
        }
        require(row.get("sourceId").type().name().equals("INTEGER") &&
                row.get("targetId").type().name().equals("INTEGER"), "Endpoint public ID must be an integer");
        return new ExactReferenceMatcher.Observed(id, Set.of(row.get("type").asString()), props,
                row.get("sourceId").asLong(), row.get("targetId").asLong(),
                labels(row.get("sourceLabels")), labels(row.get("targetLabels")));
    }

    public static String line(InputStream stream, int maxBytes) throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        for (int value; (value = stream.read()) != -1;) {
            require(raw.size() < maxBytes, "CSV row exceeds frozen byte budget");
            if (value == '\n') {
                return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(raw.toByteArray())).toString();
            }
            raw.write(value);
        }
        require(raw.size() == 0, "CSV lacks terminal LF");
        return null;
    }

    private Map<String, Object> table(Session session, JsonNode table, ExactReferenceMatcher.Bitmap visited)
            throws Exception {
        ExactReferenceMatcher.Shape shape = shape(table);
        String query = query(shape);
        Path root = Path.of(this.plan.path("inputRoot").asText()).toRealPath();
        String relative = table.path("path").asText();
        Path path = root.resolve(relative).normalize();
        require(!Path.of(relative).isAbsolute() && path.startsWith(root) && path.equals(path.toRealPath()),
                "CSV path escapes root or uses symbolic links");
        BasicFileAttributes before = Files.readAttributes(path, BasicFileAttributes.class);
        long expectedBytes = limit(table, "bytes", 1, Long.MAX_VALUE);
        long expectedRows = limit(table, "rows", 0, Long.MAX_VALUE);
        require(before.isRegularFile() && before.size() == expectedBytes, "CSV length differs from manifest");
        JsonNode budget = this.plan.path("budget");
        int maxRow = (int) limit(budget, "maxRowBytes", 1, 4 * 1024 * 1024);
        int batchSize = (int) limit(budget, "batchSize", 1, 10000);
        long maxBatch = limit(budget, "maxBatchBytes", maxRow, 64 * 1024 * 1024);
        int candidates = (int) limit(budget, "maxCandidatesPerRow", 1, 10000);
        MessageDigest sourceHash = MessageDigest.getInstance("SHA-256");
        MessageDigest matched = MessageDigest.getInstance("SHA-256");
        long rows = 0;
        long batches = 0;
        long firstClaimed = visited.count();
        try (InputStream input = new BufferedInputStream(
                new DigestInputStream(Files.newInputStream(path), sourceHash))) {
            String header = line(input, maxRow);
            require(header != null && ExactReferenceMatcher.parseLine(header).stream()
                    .map(ExactReferenceMatcher.Field::value).toList().equals(shape.columns()), "CSV header changed");
            boolean eof = false;
            while (!eof) {
                guard();
                List<ExactReferenceMatcher.Expected> expected = new ArrayList<>();
                List<Map<String, Object>> parameters = new ArrayList<>();
                long batchBytes = 0;
                while (expected.size() < batchSize && batchBytes < maxBatch) {
                    String source = line(input, maxRow);
                    if (source == null) { eof = true; break; }
                    batchBytes += source.getBytes(StandardCharsets.UTF_8).length + 1;
                    require(batchBytes <= maxBatch + maxRow, "CSV batch byte cap exhausted");
                    var row = ExactReferenceMatcher.expected(shape, ExactReferenceMatcher.parseLine(source));
                    parameters.add(shape.kind().equals("nodes") ?
                            Map.of("ordinal", expected.size(), "id", row.nodeId()) :
                            Map.of("ordinal", expected.size(), "source", row.sourceId(), "target", row.targetId()));
                    expected.add(row);
                }
                if (expected.isEmpty()) { break; }
                var match = new ExactReferenceMatcher.Batch(shape, expected, visited, candidates);
                Result response = session.run(query, Map.of("rows", parameters), transaction());
                while (response.hasNext()) {
                    guard();
                    Record row = response.next();
                    match.accept(row.get("ordinal").asInt(), observed(row, shape.kind().equals("nodes")));
                }
                require(!response.consume().counters().containsUpdates(), "Read query reported graph writes");
                long[] ids = match.finish();
                for (long id : ids) {
                    matched.update(ByteBuffer.allocate(16).putLong(rows++).putLong(id).array());
                }
                batches++;
                require(rows <= expectedRows, "CSV contains extra rows");
                if (batches % 1000 == 0) {
                    snapshot(session);
                    System.out.println(JSON.writeValueAsString(Map.of("event", "reference-exact-progress",
                            "table", table.path("table").asText(), "rows", rows, "batches", batches)));
                }
            }
        }
        String hash = HexFormat.of().formatHex(sourceHash.digest());
        BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class);
        require(rows == expectedRows && rows == visited.count() - firstClaimed,
                "Full-table row/bijection count mismatch");
        require(hash.equals(table.path("sha256").asText()), "Full CSV SHA-256 differs");
        require(before.size() == after.size() && before.lastModifiedTime().equals(after.lastModifiedTime()) &&
                java.util.Objects.equals(before.fileKey(), after.fileKey()), "CSV identity changed while validating");
        return Map.of("table", table.path("table").asText(), "rows", rows, "batches", batches,
                      "csvSha256", hash, "matchedInternalIdsSha256", HexFormat.of().formatHex(matched.digest()),
                      "querySha256", sha(query.getBytes(StandardCharsets.UTF_8)));
    }

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void save(Path path, Object value) throws IOException {
        byte[] bytes = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value);
        require(bytes.length < 8 * 1024 * 1024, "Evidence JSON exceeds 8 MiB");
        Files.write(path, bytes, StandardOpenOption.CREATE_NEW);
    }

    private void verify(Session session) throws Exception {
        guard();
        Map<String, Object> before = snapshot(session);
        save(this.output.resolve("before.json"), before);
        List<Map<String, Object>> tables = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        JsonNode budget = this.plan.path("budget");
        long expectedNodes = limit(this.plan.path("expected"), "nodes", 1, Long.MAX_VALUE);
        long expectedEdges = limit(this.plan.path("expected"), "relationships", 1, Long.MAX_VALUE);
        require(this.plan.path("tables").size() == 31, "Full 31-table reference inventory required");
        long nodeClaims;
        long edgeClaims;
        try (var nodes = new ExactReferenceMatcher.Bitmap(this.output.resolve("nodes.bits"),
                    limit(budget, "maxNodeId", 0, Integer.MAX_VALUE));
             var edges = new ExactReferenceMatcher.Bitmap(this.output.resolve("relationships.bits"),
                    limit(budget, "maxRelationshipId", 0, Integer.MAX_VALUE))) {
            for (JsonNode table : this.plan.path("tables")) {
                require(seen.add(table.path("table").asText()), "Duplicate reference table");
                snapshot(session);
                Map<String, Object> receipt = table(session, table,
                        table.path("kind").asText().equals("nodes") ? nodes : edges);
                tables.add(receipt);
                save(this.output.resolve("table-" + tables.size() + ".json"), receipt);
            }
            nodeClaims = nodes.verifyCardinality();
            edgeClaims = edges.verifyCardinality();
            require(nodeClaims == expectedNodes && edgeClaims == expectedEdges, "Full graph bijection count differs");
        }
        Map<String, Object> after = snapshot(session);
        require(before.equals(after), "Database identity/counts/schema changed during validation");
        save(this.output.resolve("after.json"), after);
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("schemaVersion", 1); receipt.put("kind", "reference-exact-verification");
        receipt.put("nodesMatched", nodeClaims); receipt.put("relationshipsMatched", edgeClaims);
        receipt.put("tables", tables); receipt.put("allRowsCompared", true);
        receipt.put("referenceAnswersGenerated", false); receipt.put("performanceAccepted", false);
        receipt.put("scope", "Exact CSV/database property and typed-endpoint bijection " +
                    "under externally frozen read-only deployment");
        save(this.output.resolve("result.json"), receipt);
        Files.delete(this.output.resolve("nodes.bits"));
        Files.delete(this.output.resolve("relationships.bits"));
    }

    private static Driver driver(String endpoint) {
        URI uri = URI.create(endpoint);
        require(uri.getScheme().equals("bolt") && Set.of("127.0.0.1", "localhost", "[::1]").contains(uri.getHost()) &&
                uri.getPort() > 0 && uri.getUserInfo() == null && uri.getQuery() == null && uri.getFragment() == null &&
                (uri.getPath() == null || uri.getPath().isEmpty()),
                "Use an isolated loopback Bolt URI without credentials");
        String user = System.getenv("HGBENCH_NEO4J_USER");
        var auth = user == null ? AuthTokens.none() : AuthTokens.basic(user,
                java.util.Objects.requireNonNull(System.getenv("HGBENCH_NEO4J_PASSWORD")));
        return GraphDatabase.driver(uri, auth, Config.builder().withoutEncryption().withMaxConnectionPoolSize(1)
                .withConnectionTimeout(5, TimeUnit.SECONDS).withConnectionAcquisitionTimeout(10, TimeUnit.SECONDS)
                .withMaxTransactionRetryTime(0, TimeUnit.SECONDS).withFetchSize(512).build());
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.println("ReferenceGraphVerifier inspect --uri bolt://127.0.0.1:PORT " +
                    "--database neo4j --out NEW_FILE\n" +
                    "ReferenceGraphVerifier verify --plan PLAN --plan-sha256 SHA256 --out NEW_DIRECTORY\n" +
                    "Uses all 31 frozen SF100 CSV tables. Exact properties, typed endpoints and ID bitmaps.\n" +
                    "Requires read-only DB, frozen identity/lease and complete constraints/indexes.\n" +
                    "External CPU/memory/deadline supervision is mandatory.\n" +
                    "No query retry or resume; failures stay incomplete. inspect reads metadata only; no writes.");
            return;
        }
        require(args.length == 7, "Expected one mode and three named arguments; use --help");
        Map<String, String> options = new TreeMap<>();
        for (int i = 1; i < args.length; i += 2) {
            require(options.put(args[i], args[i + 1]) == null, "Duplicate option");
        }
        Path output = Path.of(options.get("--out")).toAbsolutePath();
        if (args[0].equals("inspect")) {
            require(options.keySet().equals(Set.of("--uri", "--database", "--out")), "Invalid inspect options");
            try (Driver driver = driver(options.get("--uri"));
                 Session session = driver.session(SessionConfig.builder().withDatabase(options.get("--database"))
                        .withDefaultAccessMode(AccessMode.READ).build())) {
                save(output, inspect(session));
            }
        } else {
            require(args[0].equals("verify") && options.keySet().equals(Set.of("--plan", "--plan-sha256", "--out")),
                    "Invalid verify options");
            byte[] raw;
            try (InputStream source = Files.newInputStream(Path.of(options.get("--plan")))) {
                raw = source.readNBytes(8 * 1024 * 1024 + 1);
            }
            require(raw.length <= 8 * 1024 * 1024 && sha(raw).equals(options.get("--plan-sha256")),
                    "Plan identity changed");
            JsonNode plan = JSON.readTree(raw);
            require(plan.path("schemaVersion").asInt() == 1 &&
                    plan.path("kind").asText().equals("reference-exact-plan") &&
                    plan.path("scale").asText().equals("100"), "Expected complete frozen SF100 verification plan");
            Files.createDirectory(output);
            Files.write(output.resolve("plan.json"), raw, StandardOpenOption.CREATE_NEW);
            try (Driver driver = driver(plan.path("uri").asText());
                 Session session = driver.session(SessionConfig.builder().withDatabase(plan.path("database").asText())
                        .withDefaultAccessMode(AccessMode.READ).build())) {
                new ReferenceGraphVerifier(plan, output).verify(session);
            } catch (Exception failure) {
                save(output.resolve("failure.json"), Map.of("error", failure.getClass().getSimpleName(),
                        "message", String.valueOf(failure.getMessage()).substring(0,
                                Math.min(1024, String.valueOf(failure.getMessage()).length())), "completed", false));
                throw failure;
            }
        }
        System.out.println("{\"ok\":true,\"performanceAccepted\":false}");
    }
}
