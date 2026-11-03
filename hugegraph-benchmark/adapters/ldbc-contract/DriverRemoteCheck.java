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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.validation.DbValidationResult;
import org.ldbcouncil.snb.driver.validation.DbValidator;
import org.ldbcouncil.snb.driver.validation.ValidationParamsFromCsvRows;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;

/** Official validator against a separately running real HugeGraph Gremlin HTTP service with unit data. */
public final class DriverRemoteCheck {
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.httpclient.disableRetryConnect", "true");
        System.setProperty("jdk.httpclient.enableAllMethodRetry", "false");
        ObjectMapper json = new ObjectMapper();
        JsonNode fixture = json.readTree(Files.readString(Path.of(args[0])));
        JsonNode ready = json.readTree(Files.readString(Path.of(args[2])));
        Map<Integer, JsonNode> tasks = new HashMap<>();
        json.readTree(Files.readString(Path.of(args[1]))).get("tasks").forEach(task ->
                tasks.put(task.get("driverType").intValue(), task));
        List<String[]> csv = new ArrayList<>();
        Set<Integer> types = new HashSet<>();
        for (JsonNode vector : fixture.get("operations")) {
            int type = vector.get("operationType").intValue();
            JsonNode task = tasks.get(type);
            Operation op = DriverHttpCheck.operation(task, vector.get("parameters"));
            Object rows = task.get("resultShape").asText().equals("row") ?
                          vector.get("result").get(0) : vector.get("result");
            csv.add(new String[]{json.writeValueAsString(op), type >= 1000 ? "\"-1\"" : json.writeValueAsString(rows)});
            types.add(type);
        }
        HugeGraphDb db = new HugeGraphDb();
        db.init(Map.of("hgbench.endpoint", ready.get("endpoint").asText(),
                       "hgbench.alias", ready.get("alias").asText(), "hgbench.timeoutMs", "15000",
                       "hgbench.maxResponseBytes", "1048576", "hgbench.httpThreads", "2"), null,
                LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping());
        PrintStream original = System.out;
        ByteArrayOutputStream logs = new ByteArrayOutputStream();
        DbValidationResult validated;
        try {
            System.setOut(new PrintStream(logs, true, StandardCharsets.UTF_8));
            validated = new DbValidator().validate(new ValidationParamsFromCsvRows(csv.iterator(), null),
                                                  db, csv.size(), null);
        } finally {
            System.setOut(original);
            db.close();
        }
        if (!validated.isSuccessful() || types.size() != 29) {
            throw new AssertionError(validated.resultMessage() + "\n" + logs.toString(StandardCharsets.UTF_8));
        }
        Files.writeString(Path.of(args[3]), json.writeValueAsString(Map.of(
            "scope", "official-validator-real-hugegraph-gremlin-http-memory-unit", "operations", types.size(),
            "orderedRecords", csv.size(), "alias", ready.get("alias").asText(), "officialDbValidatorPassed", true,
            "hugegraphHttpExecuted", true, "hstoreExecuted", false, "restForwardingExecuted", false,
            "authenticationValidated", false, "performanceVerdict", "not-evaluated")));
    }
}
