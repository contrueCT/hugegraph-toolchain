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

package org.apache.hugegraph.benchmark.ldbc;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.snb.driver.validation.DbValidationResult;
import org.ldbcouncil.snb.driver.validation.DbValidator;
import org.ldbcouncil.snb.driver.validation.ValidationParamsFromCsvRows;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;

/** Complete official DbValidator -> adapter -> loopback HTTP -> real Groovy scripts on a unit graph. */
public final class DriverHttpCheck {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static Object value(JsonNode node) {
        if (node.isIntegralNumber()) {
            return node.longValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        throw new IllegalArgumentException("Unexpected fixture property");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static Operation operation(JsonNode task, JsonNode parameters) throws Exception {
        ObjectNode input = JSON.createObjectNode();
        for (JsonNode field : task.get("parameters")) {
            String name = field.get("name").asText();
            String key = name;
            if (name.startsWith("personIdQ") || name.startsWith("personIdSQ")) {
                key = "personId";
            } else if (name.startsWith("person1IdQ")) {
                key = "person1Id";
            } else if (name.startsWith("person2IdQ")) {
                key = "person2Id";
            } else if (name.startsWith("messageId") || name.equals("messageForumId") ||
                       name.equals("messageRepliesId")) {
                key = "messageId";
            } else if (name.equals("locationIp")) {
                key = "locationIP";
            }
            require(parameters.has(key), "Missing unit parameter: " + key);
            input.set(field.get("constructorJsonName").asText(), parameters.get(key));
        }
        Class type = Class.forName("org.ldbcouncil.snb.driver.workloads.interactive." +
                                   task.get("driverClass").asText());
        return (Operation) JSON.treeToValue(input, type);
    }

    private static Map<String, String> properties(String endpoint, int timeout, int bytes) {
        return Map.of("hgbench.endpoint", endpoint, "hgbench.alias", "__g_DEFAULT-hgbench_unit",
                      "hgbench.timeoutMs", "" + timeout,
                      "hgbench.maxResponseBytes", "" + bytes, "hgbench.httpThreads", "2");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.httpclient.disableRetryConnect", "true");
        System.setProperty("jdk.httpclient.enableAllMethodRetry", "false");
        JsonNode fixture = JSON.readTree(Files.readString(Path.of(args[0])));
        Map<Integer, JsonNode> tasks = new HashMap<>();
        JSON.readTree(Files.readString(Path.of(args[1]))).get("tasks").forEach(task ->
                tasks.put(task.get("driverType").intValue(), task));
        String reads = Files.readString(Path.of(args[2]));
        String updates = Files.readString(Path.of(args[3]));
        GroovyClassLoader loader = new GroovyClassLoader();
        Class<?> readClass = loader.parseClass(reads);
        Class<?> updateClass = loader.parseClass(updates);
        TinkerTransactionGraph graph = TinkerTransactionGraph.open();
        var traversal = graph.traversal();
        Map<String, Vertex> vertices = new HashMap<>();
        for (JsonNode item : fixture.get("snapshot").get("vertices")) {
            Vertex vertex = graph.addVertex(T.id, item.get("id").asText(), T.label, item.get("label").asText());
            vertices.put(item.get("id").asText(), vertex);
            item.get("properties").fields().forEachRemaining(field -> {
                if (field.getValue().isArray()) {
                    field.getValue().forEach(v ->
                            vertex.property(VertexProperty.Cardinality.set, field.getKey(), value(v)));
                } else {
                    vertex.property(field.getKey(), value(field.getValue()));
                }
            });
        }
        for (JsonNode item : fixture.get("snapshot").get("edges")) {
            List<Object> properties = new ArrayList<>();
            item.get("properties").fields().forEachRemaining(field -> {
                properties.add(field.getKey());
                properties.add(value(field.getValue()));
            });
            vertices.get(item.get("outV").asText()).addEdge(item.get("label").asText(),
                    vertices.get(item.get("inV").asText()), properties.toArray());
        }
        graph.tx().commit();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        ExecutorService serverThreads = Executors.newFixedThreadPool(2);
        server.setExecutor(serverThreads);
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<String> mode = new AtomicReference<>("normal");
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        CountDownLatch releaseStalledBody = new CountDownLatch(1);
        server.createContext("/gremlin", exchange -> {
            requests.incrementAndGet();
            try {
                JsonNode request = JSON.readTree(exchange.getRequestBody());
                require(request.get("aliases").get("g").asText().equals("__g_DEFAULT-hgbench_unit"), "Wrong alias");
                int type = request.get("bindings").get("operationType").intValue();
                require(request.get("gremlin").asText().equals(type >= 1000 ? updates : reads), "Wrong query bytes");
                String state = mode.get();
                byte[] response;
                if (state.equals("redirect")) {
                    exchange.getResponseHeaders().set("Location", "/gremlin");
                    exchange.sendResponseHeaders(307, -1);
                    return;
                }
                if (state.equals("http-error")) {
                    exchange.sendResponseHeaders(503, -1);
                    return;
                }
                if (state.equals("stalled-body")) {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write('{');
                    exchange.getResponseBody().flush();
                    releaseStalledBody.await(4, TimeUnit.SECONDS);
                    return;
                }
                if (state.equals("oversize")) {
                    response = new byte[8192];
                } else if (state.equals("truncated")) {
                    response = "{\"status\":{\"code\":200}".getBytes(StandardCharsets.UTF_8);
                } else if (state.equals("trailing-json")) {
                    response = ("{\"status\":{\"code\":200},\"result\":{\"data\":[{\"messageContent\":\"ok\"," +
                                "\"messageCreationDate\":1}]}} {}").getBytes(StandardCharsets.UTF_8);
                } else if (state.equals("imprecise-weight")) {
                    response = ("{\"status\":{\"code\":200},\"result\":{\"data\":[{\"personIdsInPath\":[1,2]," +
                                "\"pathWeight\":1.0000000000000001}]}}").getBytes(StandardCharsets.UTF_8);
                } else if (state.equals("duplicate-json-key")) {
                    response = ("{\"status\":{\"code\":500,\"code\":200},\"result\":{\"data\":[]}}")
                            .getBytes(StandardCharsets.UTF_8);
                } else if (state.equals("gremlin-error")) {
                    response = JSON.writeValueAsBytes(Map.of("status", Map.of("code", 500),
                                                            "result", Map.of("data", List.of())));
                } else if (state.equals("false-commit")) {
                    response = JSON.writeValueAsBytes(Map.of("status", Map.of("code", 200), "result", Map.of("data",
                            List.of(Map.of("operationType", type, "committed", false, "relationshipsCreated", 1)))));
                } else {
                    Script script = (Script) (type >= 1000 ? updateClass : readClass)
                            .getDeclaredConstructor().newInstance();
                    Map parameters = JSON.convertValue(request.get("bindings").get("parameters"), Map.class);
                    script.setBinding(new Binding(Map.of("g", traversal, "operationType", type,
                                                         "parameters", parameters)));
                    Object result = script.run();
                    if (graph.tx().isOpen()) {
                        graph.tx().commit();
                    }
                    if (state.equals("commit-response-lost")) {
                        return;  // Commit happened; drop the response. The adapter must not replay this POST.
                    }
                    Object rows = result;
                    if (state.equals("coerced-integer")) {
                        ObjectNode row = (ObjectNode) JSON.valueToTree(rows).get(0);
                        row.put("cityId", "33");
                        rows = List.of(row);
                    }
                    response = JSON.writeValueAsBytes(Map.of("status", Map.of("code", 200),
                                                            "result", Map.of("data", rows)));
                }
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            } catch (Throwable error) {
                serverFailure.compareAndSet(null, error);
                if (graph.tx().isOpen()) {
                    graph.tx().rollback();
                }
                try {
                    exchange.sendResponseHeaders(500, -1);
                } catch (Exception ignored) {
                    // The client may already have cancelled a response.
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/gremlin";
        List<String[]> csv = new ArrayList<>();
        Set<Integer> types = new HashSet<>();
        for (JsonNode vector : fixture.get("operations")) {
            int type = vector.get("operationType").intValue();
            JsonNode task = tasks.get(type);
            Operation op = operation(task, vector.get("parameters"));
            Object expectedRows = task.get("resultShape").asText().equals("row") ?
                                  vector.get("result").get(0) : vector.get("result");
            String expected = type >= 1000 ? "\"-1\"" : JSON.writeValueAsString(expectedRows);
            csv.add(new String[]{JSON.writeValueAsString(op), expected});
            types.add(type);
        }
        List<String> faultCases = new ArrayList<>();
        try {
            HugeGraphDb db = new HugeGraphDb();
            db.init(properties(endpoint, 10000, 1048576), null,
                    LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping());
            PrintStream originalOutput = System.out;
            ByteArrayOutputStream logs = new ByteArrayOutputStream();
            DbValidationResult validated;
            try {
                System.setOut(new PrintStream(logs, true, StandardCharsets.UTF_8));
                validated = new DbValidator().validate(new ValidationParamsFromCsvRows(csv.iterator(), null),
                                                       db, csv.size(), null);
            } finally {
                System.setOut(originalOutput);
                db.close();
            }
            require(validated.isSuccessful(), validated.resultMessage() + "\n" + logs.toString(StandardCharsets.UTF_8));
            require(types.size() == 29, "Incomplete operation coverage");
            require(serverFailure.get() == null, "Loopback query failure: " + serverFailure.get());
            Operation read = operation(tasks.get(101), JSON.readTree("{\"personId\":1}"));
            Operation update = operation(tasks.get(1008), JSON.readTree(
                    "{\"person1Id\":6,\"person2Id\":7,\"creationDate\":8000}"));
            for (String fault : List.of("redirect", "http-error", "oversize", "truncated", "gremlin-error",
                    "duplicate-json-key", "trailing-json", "imprecise-weight", "coerced-integer", "false-commit",
                    "stalled-body", "commit-response-lost")) {
                mode.set(fault);
                int before = requests.get();
                long started = System.nanoTime();
                try (HugeGraphDb.Connection state = new HugeGraphDb.Connection(properties(endpoint,
                        fault.equals("stalled-body") ? 250 : 5000, fault.equals("oversize") ? 128 : 1048576))) {
                    ResultReporter reporter = new ResultReporter.SimpleResultReporter(new ConcurrentErrorReporter());
                    Operation op = fault.equals("false-commit") || fault.equals("commit-response-lost") ? update :
                            fault.equals("trailing-json") ?
                                    operation(tasks.get(104), JSON.readTree("{\"messageId\":1}")) :
                            fault.equals("imprecise-weight") ? operation(tasks.get(14), JSON.readTree(
                                    "{\"person1Id\":1,\"person2Id\":2}")) : read;
                    boolean failed = false;
                    try {
                        new HugeGraphDb.Handler().executeOperation(op, state, reporter);
                    } catch (Exception expected) {
                        failed = true;
                    }
                    require(failed && reporter.result() == null, "Invalid operation reported success: " + fault);
                    require(requests.get() == before + 1, "Operation was retried or redirected: " + fault);
                    try {
                        new HugeGraphDb.Handler().executeOperation(read, state, reporter);
                        throw new AssertionError("Failed attempt continued");
                    } catch (org.ldbcouncil.snb.driver.DbException expected) {
                        require(requests.get() == before + 1, "Poisoned attempt contacted database");
                    }
                } finally {
                    if (fault.equals("stalled-body")) {
                        releaseStalledBody.countDown();
                        require(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2), "Full-body deadline failed");
                    }
                }
                faultCases.add(fault);
            }
            require(traversal.V("person:6").both("person_knows_person").hasId("person:7").hasNext(),
                    "Unknown-commit test did not actually commit its transaction");
            graph.tx().rollback();
            Map<String, Object> report = new HashMap<>();
            report.put("scope", "official-validator-through-http-unit-backend");
            report.put("operations", types.size());
            report.put("orderedRecords", csv.size());
            report.put("faultCases", faultCases);
            report.put("officialDbValidatorPassed", true);
            report.put("unknownCommitNotRetried", true);
            report.put("hugegraphExecuted", false);
            report.put("performanceVerdict", "not-evaluated");
            new PrintWriter(System.out, true).println(JSON.writeValueAsString(report));
        } finally {
            releaseStalledBody.countDown();
            server.stop(0);
            serverThreads.shutdownNow();
            serverThreads.awaitTermination(2, TimeUnit.SECONDS);
            graph.close();
            loader.close();
        }
    }
}
