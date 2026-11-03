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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser;
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory;
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator;
import scala.Option;

/** Parse frozen statements using the actual reference distribution, without a database. */
public final class CypherSyntaxCheck {
    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length > 64) {
            throw new IllegalArgumentException("Supply 1..64 complete single-statement files");
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        boolean accepted = true;
        for (String name : args) {
            Path path = Path.of(name);
            if (Files.size(path) > 1048576) {
                throw new IllegalArgumentException("Statement exceeds 1 MiB: " + path);
            }
            byte[] bytes = Files.readAllBytes(path);
            String query = new String(bytes, StandardCharsets.UTF_8);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("file", path.getFileName().toString());
            row.put("sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
            try {
                var statement = JavaCCParser.parse(query, new Neo4jCypherExceptionFactory(query, Option.empty()),
                                                  new AnonymousVariableNameGenerator());
                row.put("accepted", true);
                row.put("astType", statement.getClass().getName());
            } catch (Exception error) {
                accepted = false;
                row.put("accepted", false);
                row.put("error", error.getClass().getName());
                row.put("message", String.valueOf(error.getMessage()));
            }
            rows.add(row);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schemaVersion", 1);
        result.put("scope", "syntax-only; no query execution or database validation");
        result.put("allAccepted", accepted);
        result.put("statements", rows);
        result.put("databaseStarted", false);
        result.put("performanceAccepted", false);
        new java.io.PrintWriter(System.out, true).println(new ObjectMapper().writeValueAsString(result));
        if (!accepted) {
            System.exit(4);
        }
    }
}
