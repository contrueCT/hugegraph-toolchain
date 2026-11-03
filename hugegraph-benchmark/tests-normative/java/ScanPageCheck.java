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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.script.Bindings;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

/** Isolated functional fixture; not a dataset, HStore or performance experiment. */
public class ScanPageCheck {
    public static void main(String[] args) throws Exception {
        System.out.printf("HUGEGRAPH_CLASS_SOURCE=%s%n",
                          HugeGraph.class.getProtectionDomain().getCodeSource().getLocation());
        RegisterUtil.registerBackends();
        HugeGraph graph = HugeFactory.open(new HugeConfig(Map.of(
            "backend", "rocksdb", "serializer", "binary", "store", "scan_page_check",
            "rocksdb.data_path", args[2], "rocksdb.wal_path", args[2],
            "rocksdb.block_cache_capacity", "8388608",
            "gremlin.graph", "org.apache.hugegraph.HugeFactory")));
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        ObjectMapper mapper = new ObjectMapper();
        List<Map<?, ?>> pages = new ArrayList<>();
        int passed = 0;
        try {
            graph.initBackend();
            graph.schema().propertyKey("epoch_marker").asText().create();
            graph.schema().propertyKey("ldbcId").asLong().create();
            graph.schema().propertyKey("edgeKey").asLong().create();
            graph.schema().propertyKey("firstName").asText().create();
            graph.schema().propertyKey("stamp").asText().create();
            graph.schema().propertyKey("emails").asText().valueSet().create();
            graph.schema().vertexLabel("person").useCustomizeStringId()
                 .properties("ldbcId", "firstName", "emails").create();
            graph.schema().edgeLabel("knows").sourceLabel("person").targetLabel("person")
                 .multiTimes().properties("edgeKey", "stamp").sortKeys("edgeKey").create();
            Vertex a = graph.addVertex(T.id, "person:9007199254740993>甲", T.label, "person",
                                      "ldbcId", 9007199254740993L, "firstName", " \t甲\n ",
                                      "emails", List.of("b", "a"));
            Vertex b = graph.addVertex(T.id, "person:2", T.label, "person", "ldbcId", 2L,
                                      "firstName", "B", "emails", List.of("c"));
            for (long key : new long[]{0L, -1L, 9007199254740993L, Long.MAX_VALUE}) {
                a.addEdge("knows", b, "edgeKey", key, "stamp", "甲>" + key);
            }
            b.addEdge("knows", a, "edgeKey", 7L, "stamp", "reverse");
            graph.tx().commit();
            String script = Files.readString(Path.of(args[0]));
            String schema = Files.readString(Path.of(args[1]));
            Bindings values = engine.createBindings();
            values.put("g", graph.traversal());
            values.put("graphName", graph.name());
            values.put("epochMarker", "epoch_marker");
            Object wirePageSize = httpPageSize("1");
            if (!(wirePageSize instanceof Long) || !wirePageSize.equals(1L)) {
                throw new AssertionError("Gremlin HTTP integral binding did not decode as Long");
            }
            values.put("pageSize", wirePageSize);
            values.put("page", "");
            values.put("category", "schema");
            Object before = evaluate(engine, values, schema);
            passed++;
            for (String category : List.of("vertices", "edges")) {
                values.put("category", category);
                String page = "";
                Set<String> seen = new HashSet<>();
                int rows = 0;
                int requests = 0;
                do {
                    if (!seen.add(page) || ++requests > 12) throw new AssertionError("Cursor cycle/budget");
                    values.put("page", page);
                    List<?> result = (List<?>) evaluate(engine, values, script);
                    if (result.size() != 1) throw new AssertionError("Envelope cardinality");
                    Map<?, ?> response = (Map<?, ?>) result.get(0);
                    if (!page.equals(response.get("page")) || !category.equals(response.get("category")) ||
                        !"0".equals(response.get("hiddenRecords"))) throw new AssertionError("Page binding");
                    int count = ((List<?>) response.get("rows")).size();
                    if (count > 1 || !Integer.toString(count).equals(response.get("scanned"))) {
                        throw new AssertionError("Page record bound");
                    }
                    rows += count;
                    pages.add(response);
                    page = (String) response.get("nextPage");
                } while (page != null);
                if (rows != (category.equals("vertices") ? 2 : 5) || requests < 2) {
                    throw new AssertionError("Full scan cardinality/paging: " + category + "/" + rows);
                }
                passed++;
            }
            values.put("category", "schema");
            Object after = evaluate(engine, values, schema);
            if (!before.equals(after)) throw new AssertionError("Read-only scan changed schema");
            passed++;
            values.put("category", "vertices");
            values.put("page", "");
            values.put("pageSize", 1);
            evaluate(engine, values, script);
            passed++;
            values.put("epochMarker", "wrong");
            rejects(engine, values, script, "Target graph or restore epoch mismatch");
            passed++;
            values.put("epochMarker", "epoch_marker");
            values.put("graphName", "wrong");
            rejects(engine, values, script, "Target graph or restore epoch mismatch");
            passed++;
            values.put("graphName", graph.name());
            values.put("category", "unexpected");
            rejects(engine, values, script, "Invalid scan page request");
            passed++;
            values.put("category", "vertices");
            values.put("pageSize", 0);
            rejects(engine, values, script, "Invalid scan page request");
            passed++;
            values.put("pageSize", 100001);
            rejects(engine, values, script, "Invalid scan page request");
            passed++;
            for (String token : List.of("0", "100001", "-9223372036854775808", "9223372036854775807",
                                        "1.0", "\"1\"", "true", "null")) {
                values.put("pageSize", httpPageSize(token));
                rejects(engine, values, script, "Invalid scan page request");
                passed++;
            }
            Files.writeString(Path.of(args[3]), pages.stream().map(row -> {
                try {
                    return mapper.writeValueAsString(row) + "\n";
                } catch (Exception error) {
                    throw new IllegalStateException(error);
                }
            }).reduce("", String::concat));
        } finally {
            graph.close();
            HugeFactory.shutdown(5L, true);
        }
        System.out.printf("SCAN_PAGE_CHECK_PASSED=%d%n", passed);
    }

    private static Object httpPageSize(String token) {
        String body = "{\"gremlin\":\"g\",\"bindings\":{\"pageSize\":" + token + "}}";
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, "/gremlin",
                Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        try {
            Map<?, ?> bindings = (Map<?, ?>) HttpHandlerUtil.getRequestMessageFromHttpRequest(request)
                                                            .getArgs().get("bindings");
            return bindings.get("pageSize");
        } finally {
            request.release();
        }
    }

    private static void rejects(GremlinGroovyScriptEngine engine, Bindings values, String script, String message)
            throws Exception {
        try {
            evaluate(engine, values, script);
            throw new AssertionError("Invalid input accepted: " + message);
        } catch (javax.script.ScriptException expected) {
            if (!expected.getMessage().contains(message)) throw expected;
        }
    }

    @SuppressWarnings("removal")
    private static Object evaluate(GremlinGroovyScriptEngine engine, Bindings values, String script)
            throws Exception {
        engine.compile(script);
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        SecurityManager manager = System.getSecurityManager();
        try {
            thread.setName("gremlin-server-exec-scan-contract");
            System.setSecurityManager(new HugeSecurityManager());
            return engine.eval(script, values);
        } finally {
            System.setSecurityManager(manager);
            thread.setName(name);
        }
    }
}

