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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Validate before Jackson DTO decoding, which would otherwise allow numeric coercion/default fields. */
public final class ResultContract {
    private final Map<Integer, JsonNode> tasks = new HashMap<>();
    private final JsonNode organizations;

    public ResultContract() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream input = ResultContract.class.getResourceAsStream("/tasks.json")) {
            if (input == null) {
                throw new IOException("Missing bundled operation contract");
            }
            JsonNode catalog = mapper.readTree(input);
            for (JsonNode task : catalog.get("tasks")) {
                tasks.put(task.get("driverType").intValue(), task);
            }
            organizations = catalog.get("resultOrganizationFields");
        }
        if (tasks.size() != 29) {
            throw new IOException("Expected all 29 operation contracts");
        }
    }

    public Set<Integer> operationTypes() {
        return Set.copyOf(tasks.keySet());
    }

    public String className(int type) {
        return tasks.get(type).get("driverClass").asText();
    }

    public JsonNode validate(int type, JsonNode data, Map<String, Object> parameters) throws IOException {
        JsonNode task = tasks.get(type);
        require(task != null && data.isArray(), "Unknown operation or non-array response");
        if (type >= 1000) {
            require(data.size() == 1, "Expected exactly one update acknowledgment");
            JsonNode ack = data.get(0);
            require(ack.isObject() && ack.size() == 3 && ack.path("committed").isBoolean() &&
                    ack.get("committed").booleanValue() && ack.path("operationType").isIntegralNumber() &&
                    ack.get("operationType").canConvertToInt() && ack.get("operationType").intValue() == type &&
                    ack.path("relationshipsCreated").isIntegralNumber() &&
                    ack.get("relationshipsCreated").canConvertToInt() &&
                    ack.get("relationshipsCreated").intValue() > 0, "Unconfirmed/mismatched update commit");
            return null;
        }
        if (parameters.containsKey("limit")) {
            require(data.size() <= ((Number) parameters.get("limit")).intValue(), "Result exceeds requested limit");
        }
        for (JsonNode row : data) {
            row(row, task.get("resultFields"));
        }
        if ("row".equals(task.get("resultShape").asText())) {
            require(data.size() == 1, "Expected exactly one complete result row");
            return data.get(0);
        }
        return data;
    }

    private void row(JsonNode value, JsonNode fields) throws IOException {
        require(value.isObject(), "Expected result object");
        Set<String> expected = new HashSet<>();
        for (JsonNode field : fields) {
            String name = field.get("name").asText();
            expected.add(name);
            typed(value.get(name), field.get("javaType").asText());
        }
        Set<String> actual = new HashSet<>();
        value.fieldNames().forEachRemaining(actual::add);
        require(actual.equals(expected), "Unexpected/missing result fields");
    }

    private void typed(JsonNode value, String type) throws IOException {
        require(value != null && !value.isNull(), "Missing/null result field");
        switch (type) {
            case "long":
                require(value.isIntegralNumber() && value.canConvertToLong(), "Expected exact signed 64-bit integer");
                break;
            case "int":
                require(value.isIntegralNumber() && value.canConvertToInt(), "Expected exact signed 32-bit integer");
                break;
            case "String":
                require(value.isTextual(), "Expected string");
                break;
            case "boolean":
                require(value.isBoolean(), "Expected boolean");
                break;
            case "double":
                require(value.isNumber() && Double.isFinite(value.doubleValue()) &&
                        BigDecimal.valueOf(value.doubleValue()).compareTo(value.decimalValue()) == 0,
                        "Expected exactly representable finite IC14 weight");
                BigDecimal half = value.decimalValue().multiply(BigDecimal.valueOf(2));
                require(half.signum() >= 0 && half.stripTrailingZeros().scale() <= 0,
                        "Expected nonnegative exact half-integer IC14 weight");
                break;
            case "Iterable<String>":
            case "Iterable<Organization>":
            case "Iterable<? extends Number>":
                require(value.isArray(), "Expected collection");
                for (JsonNode item : value) {
                    if ("Iterable<Organization>".equals(type)) {
                        row(item, organizations);
                    } else {
                        typed(item, "Iterable<String>".equals(type) ? "String" : "long");
                    }
                }
                break;
            default:
                throw new IOException("Unknown published result field type");
        }
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }
}
