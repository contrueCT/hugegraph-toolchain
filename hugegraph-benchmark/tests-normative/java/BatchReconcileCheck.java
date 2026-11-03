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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.script.Bindings;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class BatchReconcileCheck {
    public static void main(String[] args) throws Exception {
        System.out.printf("HUGEGRAPH_CLASS_SOURCE=%s%n",
                          HugeGraph.class.getProtectionDomain().getCodeSource().getLocation());
        RegisterUtil.registerBackends();
        HugeGraph graph = HugeFactory.open(new HugeConfig(Map.of(
            "backend", "memory", "serializer", "text", "store", "batch_reconcile_check",
            "gremlin.graph", "org.apache.hugegraph.HugeFactory")));
        int passed = 0;
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        try {
            graph.initBackend();
            graph.schema().propertyKey("epoch_marker").asText().create();
            graph.schema().propertyKey("firstName").asText().create();
            graph.schema().propertyKey("ldbcId").asLong().create();
            graph.schema().propertyKey("edgeKey").asLong().create();
            graph.schema().propertyKey("stamp").asText().create();
            graph.schema().vertexLabel("person").useCustomizeStringId()
                 .properties("firstName", "ldbcId").create();
            graph.schema().edgeLabel("knows").sourceLabel("person").targetLabel("person")
                 .multiTimes().properties("edgeKey", "stamp").sortKeys("edgeKey").create();
            graph.schema().edgeLabel("single").sourceLabel("person").targetLabel("person")
                 .properties("stamp").create();
            String a = "person:9007199254740993>甲";
            String b = "person:2";
            Vertex v = graph.addVertex(T.id, a, T.label, "person", "firstName", " \t甲\n ", "ldbcId", 9007199254740993L);
            Vertex w = graph.addVertex(T.id, b, T.label, "person", "firstName", "B", "ldbcId", 2L);
            long[] keys = {0L, -1L, 9007199254740993L, Long.MAX_VALUE};
            for (long key : keys) v.addEdge("knows", w, "edgeKey", key, "stamp", "甲>" + key);
            graph.tx().commit();
            String single = Files.readString(Path.of(args[0]));
            String batch = Files.readString(Path.of(args[1]));
            Bindings values = engine.createBindings();
            values.put("g", graph.traversal());
            values.put("graphName", graph.name());
            values.put("epochMarker", "epoch_marker");
            values.put("category", "vertices");
            values.put("label", "person");
            for (List<Map<String, Object>> selectors : List.of(
                    List.of(Map.<String, Object>of("id", b), Map.<String, Object>of("id", a),
                            Map.<String, Object>of("id", "missing"), Map.<String, Object>of("id", a)),
                    List.<Map<String, Object>>of())) {
                values.put("selectors", selectors);
                compare(engine, values, single, batch);
                passed++;
            }
            values.put("category", "edges");
            values.put("label", "knows");
            List<Map<String, Object>> selectors = new ArrayList<>();
            for (long key : keys) selectors.add(Map.of("sourceId", a, "targetId", b, "edgeKey", key));
            selectors.add(selectors.get(0));
            selectors.add(Map.of("sourceId", a, "targetId", b, "edgeKey", 77L));
            values.put("selectors", selectors);
            compare(engine, values, single, batch);
            passed++;
            if (args.length == 3) {
                // Consume selectors exported by the real Python producer, not hand-built Java maps.
                List<Map<String, Object>> external = new ObjectMapper().readValue(
                        Files.readString(Path.of(args[2])), new TypeReference<>() { });
                values.put("selectors", external);
                compare(engine, values, single, batch);
                List<?> observed = rows(evaluate(engine, values, batch));
                if (observed.size() != 6) throw new AssertionError("Expected six Python producer selectors");
                for (int i = 0; i < 5; i++) {
                    if (((List<?>) observed.get(i)).size() != 1) {
                        throw new AssertionError("Python producer failed to locate an existing edge");
                    }
                }
                if (!((List<?>) observed.get(5)).isEmpty()) {
                    throw new AssertionError("Python producer's absent edge was fabricated");
                }
                passed++;
            }
            values.put("selectors", List.of());
            compare(engine, values, single, batch);
            passed++;
            values.put("epochMarker", "wrong_epoch");
            rejects(engine, values, batch, "Target graph or restore epoch mismatch");
            passed++;
            values.put("epochMarker", "epoch_marker");
            values.put("selectors", List.of(
                Map.of("sourceId", b, "targetId", a, "edgeKey", 0L),
                Map.of("sourceId", a, "targetId", "missing", "edgeKey", 0L)));
            Object missing = evaluate(engine, values, batch);
            if (!rows(missing).equals(List.of(List.of(), List.of()))) {
                throw new AssertionError("Wrong source/destination did not remain absent");
            }
            passed++;
            values.put("category", "unexpected");
            rejects(engine, values, batch, "Unexpected record category");
            passed++;
            values.put("category", "edges");
            values.put("label", "single");
            rejects(engine, values, batch, "Expected standalone edge label with edgeKey sort key");
            passed++;
            values.put("category", "vertices");
            values.put("label", "person");
            values.put("selectors", List.of(Map.of("id", a)));
            values.put("g", new SuppliedResults(graph, List.of(v, w)));
            rejects(engine, values, batch, "Unrequested ID observation");
            passed++;
            values.put("g", new SuppliedResults(graph, List.of(v, v)));
            if (rows(evaluate(engine, values, batch)).size() != 1) {
                throw new AssertionError("Repeated identical result changed selector cardinality");
            }
            passed++;
        } finally {
            graph.close();
            HugeFactory.shutdown(5L, true);
        }
        System.out.printf("BATCH_RECONCILE_CHECK_PASSED=%d%n", passed);
    }

    // Only the unexpected/duplicated provider-result checks inject a traversal result.
    // All ordinary lookups above use the native HugeGraph memory backend.
    public static final class SuppliedResults {
        private final HugeGraph graph;
        private final List<Vertex> vertices;

        public SuppliedResults(HugeGraph graph, List<Vertex> vertices) {
            this.graph = graph;
            this.vertices = vertices;
        }

        public HugeGraph getGraph() { return this.graph; }
        public SuppliedResults V(Object... ids) { return this; }
        public List<Vertex> toList() { return this.vertices; }
    }

    private static List<?> rows(Object response) {
        return (List<?>) ((Map<?, ?>) ((List<?>) response).get(0)).get("rows");
    }

    private static void rejects(GremlinGroovyScriptEngine engine, Bindings values, String script, String message)
            throws Exception {
        try {
            evaluate(engine, values, script);
            throw new AssertionError("Invalid input was accepted: " + message);
        } catch (javax.script.ScriptException expected) {
            if (!expected.getMessage().contains(message)) throw expected;
        }
    }

    private static void compare(GremlinGroovyScriptEngine engine, Bindings values, String single, String batch)
            throws Exception {
        Object original = evaluate(engine, values, single);
        Object observed = evaluate(engine, values, batch);
        if (!original.equals(observed)) throw new AssertionError("Batch/full individual observations differ: "
                                                               + original + " vs " + observed);
    }

    @SuppressWarnings("removal")
    private static Object evaluate(GremlinGroovyScriptEngine engine, Bindings values, String script)
            throws Exception {
        engine.compile(script);
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        SecurityManager manager = System.getSecurityManager();
        try {
            thread.setName("gremlin-server-exec-batch-reconciliation");
            System.setSecurityManager(new HugeSecurityManager());
            return engine.eval(script, values);
        } finally {
            System.setSecurityManager(manager);
            thread.setName(name);
        }
    }
}
