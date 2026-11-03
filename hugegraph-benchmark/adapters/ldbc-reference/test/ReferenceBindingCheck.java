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

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.ldbcouncil.snb.impls.workloads.cypher.CypherDbConnectionState;
import org.ldbcouncil.snb.impls.workloads.cypher.interactive.CypherInteractiveDb;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

/** Real upstream classes and driver; synthetic row values only, no database or graph is executed. */
public final class ReferenceBindingCheck {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long LARGE_ID = 9007199254740993L;
    private static final long DATE = 1300000000000L;

    private static void require(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }

    private static Object example(String type, int index, boolean result) {
        switch (type) {
            case "long":
                return LARGE_ID + index;
            case "int":
                return index + 2;
            case "Date":
                return DATE;
            case "String":
                return "中文\\\"field" + index;
            case "boolean":
                return true;
            case "double":
                return 1.5;
            case "List<String>":
            case "Iterable<String>":
                return List.of("中文" + index, "value" + index);
            case "List<Long>":
            case "Iterable<? extends Number>":
                return List.of(LARGE_ID, Long.MAX_VALUE);
            case "List<Organization>":
                return List.of(Map.of("organizationId", LARGE_ID, "year", 2001));
            case "Iterable<Organization>":
                return result ? List.of(Map.of("organizationName", "org" + index,
                                               "year", 2001, "placeName", "city" + index)) :
                                List.of(List.of("org" + index, 2001L, "city" + index));
            default:
                throw new IllegalArgumentException(type);
        }
    }

    @SuppressWarnings("rawtypes")
    private static Operation operation(JsonNode task) throws Exception {
        ObjectNode body = JSON.createObjectNode();
        int index = 0;
        for (JsonNode parameter : task.get("parameters")) {
            Object value = example(parameter.get("javaType").asText(), index++, false);
            if (parameter.get("name").asText().equals("limit")) {
                value = task.get("defaultLimit").asInt();
            }
            body.set(parameter.get("constructorJsonName").asText(), JSON.valueToTree(value));
        }
        return (Operation) JSON.treeToValue(body, Class.forName(
                "org.ldbcouncil.snb.driver.workloads.interactive." + task.get("driverClass").asText()));
    }

    private static void checkResult(Object handler, JsonNode task) throws Exception {
        ObjectNode expected = JSON.createObjectNode();
        List<Value> values = new ArrayList<>();
        int index = 0;
        for (JsonNode field : task.get("resultFields")) {
            String type = field.get("javaType").asText();
            expected.set(field.get("constructorJsonName").asText(), JSON.valueToTree(example(type, index, true)));
            values.add(Values.value(example(type, index++, false)));
        }
        // The pinned IS4 RETURN clause emits creationDate before content; its result constructor reverses them.
        if (task.get("id").asText().equals("IS4")) {
            Collections.swap(values, 0, 1);
        }
        Record row = (Record) Proxy.newProxyInstance(Record.class.getClassLoader(), new Class<?>[]{Record.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("get") && arguments[0] instanceof Integer) {
                        return values.get((Integer) arguments[0]);
                    }
                    throw new UnsupportedOperationException("Unexpected row API: " + method);
                });
        Object observed = handler.getClass().getMethod("toResult", Record.class).invoke(handler, row);
        Object wanted = JSON.treeToValue(expected, Class.forName(
                "org.ldbcouncil.snb.driver.workloads.interactive." + task.get("driverClass").asText() + "Result"));
        require(wanted.equals(observed), task.get("id") + " row mapping differs: " +
                                        JSON.writeValueAsString(observed));
        require(JSON.readTree(JSON.writeValueAsString(observed)).equals(JSON.valueToTree(wanted)),
                task.get("id") + " result serialization differs");
    }

    private static void checkQueryParameters(String query, Map<?, ?> parameters, String id) {
        String body = query.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("(?m)//[^\\n]*", "");
        Matcher matcher = Pattern.compile("\\$([A-Za-z_][A-Za-z0-9_]*)").matcher(body);
        int count = 0;
        while (matcher.find()) {
            require(parameters.containsKey(matcher.group(1)), id + " missing parameter " + matcher.group(1));
            count++;
        }
        require(count > 0, id + " has no bound query parameter");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void main(String[] args) throws Exception {
        JsonNode tasks = JSON.readTree(Files.readString(Path.of(args[1]))).get("tasks");
        require(tasks.size() == 29, "Incomplete task contract");
        // Cover a DST-observing default timezone while the reference's interval arithmetic uses UTC.
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        List<String> operations = new ArrayList<>();
        List<String> rowMappings = new ArrayList<>();
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            listener.setSoTimeout(100);
            Map<String, String> properties = Map.of("endpoint", "bolt://127.0.0.1:" + listener.getLocalPort(),
                    "user", "unit-only", "password", "unit-only", "queryDir", args[0]);
            CypherInteractiveDb db = new CypherInteractiveDb();
            db.init(properties, null, LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping());
            try {
                for (JsonNode task : tasks) {
                    Operation op = operation(task);
                    int type = task.get("driverType").asInt();
                    String id = task.get("id").asText();
                    require(op.type() == type, id + " operation type differs");
                    var context = db.getOperationHandlerRunnableContext(op);
                    try {
                        Object handler = context.operationHandler();
                        CypherDbConnectionState state = (CypherDbConnectionState) context.dbConnectionState();
                        Method queryMethod = handler.getClass().getMethod("getQueryString",
                                CypherDbConnectionState.class, op.getClass());
                        String query = (String) queryMethod.invoke(handler, state, op);
                        require(query != null && !query.isBlank(), id + " query missing");
                        String expected = Files.readString(Path.of(args[0],
                                Path.of(task.get("referenceQuery").asText()).getFileName().toString()));
                        require(query.equals(expected), id + " loaded a different query");
                        Map<?, ?> parameters;
                        if (type >= 1000) {
                            parameters = (Map<?, ?>) handler.getClass().getMethod("getParameters", op.getClass())
                                    .invoke(handler, op);
                        } else {
                            parameters = (Map<?, ?>) handler.getClass().getMethod("getParameters",
                                    CypherDbConnectionState.class, op.getClass()).invoke(handler, state, op);
                            checkResult(handler, task);
                            rowMappings.add(id);
                        }
                        checkQueryParameters(query, parameters, id);
                        if (id.equals("IC3") || id.equals("IC4")) {
                            long duration = ((Number) op.parameterMap().get("durationDays")).longValue();
                            require(((Number) parameters.get("endDate")).longValue() == DATE + duration * 86400000L,
                                    id + " interval depended on local DST");
                        }
                        if (id.equals("IU1")) {
                            require(parameters.get("personId").equals(LARGE_ID), "Update ID lost precision");
                            require(parameters.get("birthday").equals(DATE), "Update date lost epoch milliseconds");
                        }
                        Operation roundTrip = (Operation) JSON.readValue(JSON.writeValueAsString(op), op.getClass());
                        require(op.equals(roundTrip), id + " operation serialization round-trip failed");
                        operations.add(id);
                    } finally {
                        context.cleanup();
                    }
                }
            } finally {
                db.close();
            }
            try (var unexpected = listener.accept()) {
                throw new AssertionError("Reference binding check opened a database connection");
            } catch (SocketTimeoutException expected) {
                // Initialization and the above mapping checks must remain offline.
            }
        }
        Map<String, Object> report = new HashMap<>();
        report.put("scope", "real-reference-driver-binding-and-synthetic-row-mapping-no-database");
        report.put("operations", operations);
        report.put("readResultMappings", rowMappings);
        report.put("registeredOperations", operations.size());
        report.put("readResultMappingCount", rowMappings.size());
        report.put("databaseConnections", 0);
        report.put("databaseExecuted", false);
        report.put("referenceAnswersGenerated", false);
        report.put("sf100Executed", false);
        report.put("executionJvm", Map.of("javaVersion", System.getProperty("java.version"),
                "runtimeVersion", System.getProperty("java.runtime.version"),
                "vendor", System.getProperty("java.vendor"),
                "vmName", System.getProperty("java.vm.name"),
                "javaHome", System.getProperty("java.home")));
        report.put("defaultTimezoneDuringCheck", TimeZone.getDefault().getID());
        Files.writeString(Path.of(args[2]), JSON.writeValueAsString(report) + "\n");
        new PrintWriter(System.out, true).println(JSON.writeValueAsString(Map.of("ok", true, "report", args[2])));
    }
}
