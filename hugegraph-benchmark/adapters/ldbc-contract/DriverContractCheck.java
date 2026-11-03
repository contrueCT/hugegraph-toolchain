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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import org.ldbcouncil.snb.driver.Operation;

/** Bounded, database-free check of actual pinned driver classes and Jackson 2.13.2. */
public final class DriverContractCheck {
    private static long valueOrdinal;

    private static Object argument(Type type) throws Exception {
        if (type instanceof ParameterizedType) {
            Type child = ((ParameterizedType) type).getActualTypeArguments()[0];
            return List.of(argument(child));
        }
        if (type == long.class || type == Long.class) {
            return 9007199254740993L + (++valueOrdinal * 7L);
        }
        if (type == int.class || type == Integer.class) {
            return 17 + (int) ++valueOrdinal;
        }
        if (type == double.class || type == Double.class) {
            return 2.5D;
        }
        if (type == boolean.class) {
            return true;
        }
        if (type == String.class) {
            return "Unicode | 张三 " + ++valueOrdinal;
        }
        if (type == Date.class) {
            return new Date(1325376000123L + (++valueOrdinal * 1000L));
        }
        if (type instanceof Class<?>) {
            return construct((Class<?>) type);
        }
        // IC14 Iterable<? extends Number> must carry exact long IDs.
        return 9007199254740993L;
    }

    private static Object construct(Class<?> type) throws Exception {
        Constructor<?> constructor = type.getConstructors()[0];
        Type[] types = constructor.getGenericParameterTypes();
        Object[] args = new Object[types.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = argument(types[i]);
        }
        return constructor.newInstance(args);
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var tasks = mapper.readTree(Files.readString(Path.of(args[0]))).get("tasks");
        List<Map<String, Object>> records = new ArrayList<>();
        int failures = 0;
        for (var task : tasks) {
            String name = task.get("driverClass").asText();
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("operationId", task.get("id").asText());
            try {
                Class<?> type = Class.forName("org.ldbcouncil.snb.driver.workloads.interactive." + name);
                Operation<?> operation = (Operation<?>) construct(type);
                String encoded = mapper.writeValueAsString(operation);
                Operation<?> restored = mapper.readValue(encoded, Operation.class);
                boolean equal = operation.getClass() == restored.getClass() && operation.equals(restored) &&
                                operation.parameterMap().equals(restored.parameterMap());
                record.put("operationRoundTrip", equal);
                // This is the actual ObjectMapper/Operation deduction path used by validation CSV parsing.
                if (!equal) {
                    record.put("originalParameters", operation.parameterMap());
                    record.put("restoredParameters", restored.parameterMap());
                    failures++;
                }
                if (!task.get("resultShape").asText().equals("none")) {
                    Object expected = construct(Class.forName(type.getName() + "Result"));
                    Object result = task.get("resultShape").asText().equals("rows") ? List.of(expected) : expected;
                    String serialized = mapper.writeValueAsString(result);
                    Object decoded = operation.deserializeResult(serialized);
                    boolean resultEqual = result.equals(decoded);
                    record.put("resultRoundTrip", resultEqual);
                    if (!resultEqual) {
                        failures++;
                    }
                    var resultTree = mapper.readTree(serialized);
                    var row = resultTree.isArray() ? resultTree.get(0) : resultTree;
                    Set<String> observedFields = new HashSet<>();
                    row.fieldNames().forEachRemaining(observedFields::add);
                    Set<String> declaredFields = new HashSet<>();
                    task.get("resultFields").forEach(field -> declaredFields.add(field.get("name").asText()));
                    boolean schemaEqual = observedFields.equals(declaredFields);
                    record.put("publishedResultFieldsMatch", schemaEqual);
                    if (!schemaEqual) {
                        failures++;
                    }
                }
            } catch (Exception error) {
                record.put("error", error.getClass().getName() + ": " + error.getMessage());
                failures++;
            }
            records.add(record);
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("scope", "pinned-driver-serialization-contract");
        report.put("operations", records.size());
        report.put("failures", failures);
        report.put("records", records);
        report.put("hugegraphExecuted", false);
        java.io.PrintWriter output = new java.io.PrintWriter(System.out, true);
        output.println(mapper.writeValueAsString(report));
        if (failures > 0) {
            System.exit(10);
        }
    }
}
