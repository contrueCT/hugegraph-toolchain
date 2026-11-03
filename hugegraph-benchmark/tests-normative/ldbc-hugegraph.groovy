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


import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.commons.configuration2.BaseConfiguration
import org.apache.hugegraph.HugeFactory
import org.apache.hugegraph.config.HugeConfig
import org.apache.hugegraph.dist.RegisterUtil
import org.apache.tinkerpop.gremlin.structure.T

// Hand-authored semantic unit data, executed by the real HugeGraph engine in isolation.
def fixture = new JsonSlurper().parse(new File(args[0]))
def shell = new GroovyShell()
def reads = shell.parse(new File(args[1])), updates = shell.parse(new File(args[2]))
RegisterUtil.registerBackends()
def config = new BaseConfiguration()
config.setProperty('backend', 'memory')
config.setProperty('serializer', 'text')
config.setProperty('store', 'hgbench_unit')
config.setProperty('query.ignore_invalid_data', false)
def graph = HugeFactory.open(new HugeConfig(config))
try {
    graph.initBackend()
    def schema = graph.schema()
    Map<String, Set> fields = [:].withDefault { new TreeSet() }
    fixture.snapshot.vertices.each { fields[it.label].addAll(it.properties.keySet()) }
    fixture.snapshot.edges.each { fields[it.label].addAll(it.properties.keySet()) }
    fields.post.addAll(['imageFile', 'language', 'browserUsed', 'locationIP'])
    fields.comment.addAll(['browserUsed', 'locationIP'])
    ['forum_hasTag_tag', 'comment_hasTag_tag'].each { fields[it].add('edgeKey') }
    Set numeric = ['ldbcId', 'creationDate', 'birthday', 'length', 'joinDate', 'classYear', 'workFrom', 'edgeKey'] as Set
    fields.values().flatten().toSet().sort().each { name ->
        def key = schema.propertyKey(name)
        if (name in numeric) key.asLong() else key.asText()
        if (name in ['emails', 'languages']) key.valueSet()
        key.create()
    }
    fields.findAll { !it.key.contains('_') }.each { name, properties ->
        schema.vertexLabel(name).useCustomizeStringId().properties(properties as String[])
              .nullableKeys((properties - ['ldbcId']) as String[]).create()
    }
    fields.findAll { it.key.contains('_') }.each { name, properties ->
        def parts = name.split('_')
        def edge = schema.edgeLabel(name).sourceLabel(parts[0]).targetLabel(parts[2]).multiTimes()
                         .properties(properties as String[]).sortKeys('edgeKey')
        def nullable = properties - ['edgeKey']
        if (nullable) edge.nullableKeys(nullable as String[])
        edge.create()
    }
    Map vertices = [:]
    fixture.snapshot.vertices.each { item ->
        List values = [T.id, item.id, T.label, item.label]
        item.properties.each { key, value ->
            values.add(key)
            values.add(key in numeric ? (value as long) : value)
        }
        vertices[item.id] = graph.addVertex(values.toArray())
    }
    fixture.snapshot.edges.each { item ->
        List values = []
        item.properties.each { key, value -> values.add(key); values.add(key in numeric ? (value as long) : value) }
        vertices[item.outV].addEdge(item.label, vertices[item.inV], values.toArray())
    }
    graph.tx().commit()
    def g = graph.traversal()
    if (args.length == 8) {
        // Real HugeGraph server/channelizer, isolated on loopback; no production deployment or authentication claim.
        int port
        new ServerSocket(0).withCloseable { port = it.localPort }
        def settings = new org.apache.tinkerpop.gremlin.server.Settings()
        settings.host = '127.0.0.1'
        settings.port = port
        settings.gremlinPool = 2
        settings.threadPoolWorker = 1
        settings.threadPoolBoss = 1
        settings.evaluationTimeout = 15000L
        settings.channelizer = 'org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer'
        settings.scriptEngines.clear()
        settings.scriptEngines.put('gremlin-groovy', new org.apache.tinkerpop.gremlin.server.Settings.ScriptEngineSettings())
        def serializer = new org.apache.tinkerpop.gremlin.server.Settings.SerializerSettings()
        serializer.className = 'org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1'
        serializer.config = [ioRegistries: ['org.apache.hugegraph.io.HugeGraphIoRegistry']]
        settings.serializers = [serializer]
        def server = new org.apache.hugegraph.auth.ContextGremlinServer(settings,
            new org.apache.hugegraph.event.EventHub('hgbench-unit-http'))
        try {
            server.serverGremlinExecutor.graphManager.putGraph(graph.spaceGraphName(), graph)
            server.injectTraversalSource()
            server.start().get(15L, java.util.concurrent.TimeUnit.SECONDS)
            new File(args[6]).text = JsonOutput.toJson([endpoint: 'http://127.0.0.1:' + port + '/gremlin',
                alias: '__g_' + graph.spaceGraphName(),
                serverClass: server.class.name, channelizer: settings.channelizer, serializer: serializer.className,
                securityMode: org.apache.hugegraph.security.script.ScriptPolicyRuntime.mode().configValue(),
                securityManagerEnabled: System.getSecurityManager() != null])
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(45L)
            while (!new File(args[7]).exists()) {
                if (System.nanoTime() > deadline) throw new IllegalStateException('HTTP unit client deadline exceeded')
                Thread.sleep(50L)
            }
        } finally {
            server.stop().get(10L, java.util.concurrent.TimeUnit.SECONDS)
        }
        return
    }
    def reconcile = shell.parse(new File(args[4]))
    String marker = 'hgb_epoch_unit'
    schema.propertyKey(marker).asText().create()
    fixture.snapshot.vertices.groupBy { it.label }.each { label, items ->
        reconcile.binding = new Binding([g: g, graphName: 'hgbench_unit', epochMarker: marker,
            category: 'vertices', label: label, selectors: items.collect { [id: it.id] }])
        def observed = reconcile.run()[0].rows
        assert observed.size() == items.size()
        items.eachWithIndex { item, index ->
            assert observed[index].size() == 1
            assert observed[index][0] == item
        }
    }
    fixture.snapshot.edges.groupBy { it.label }.each { label, items ->
        reconcile.binding = new Binding([g: g, graphName: 'hgbench_unit', epochMarker: marker,
            category: 'edges', label: label,
            selectors: items.collect { [sourceId: it.outV, edgeKey: it.properties.edgeKey] }])
        def observed = reconcile.run()[0].rows
        items.eachWithIndex { item, index ->
            assert observed[index] == [[sourceId: item.outV, targetId: item.inV, label: item.label,
                                       properties: item.properties]]
        }
    }
    def inventoryScript = shell.parse(new File(args[5]))
    def readInventory = {
        inventoryScript.binding = new Binding([g: g, graphName: 'hgbench_unit', epochMarker: marker])
        inventoryScript.run()[0].inventory
    }
    def initialInventory = readInventory()
    assert initialInventory.counts.vertices.person == fixture.snapshot.vertices.count { it.label == 'person' }.toString()
    graph.addVertex(T.id, 'person:9999998', T.label, 'person', 'ldbcId', 9999998L)
    graph.tx().commit()
    def extraVertexInventory = readInventory()
    assert extraVertexInventory.counts.vertices.person.toLong() == initialInventory.counts.vertices.person.toLong() + 1L
    g.V('person:9999998').drop().iterate()
    graph.tx().commit()
    schema.propertyKey('hgbench_unplanned').asText().create()
    def extraSchemaInventory = readInventory()
    assert extraSchemaInventory.schema.propertyKeys.containsKey('hgbench_unplanned')
    assert !initialInventory.schema.propertyKeys.containsKey('hgbench_unplanned')
    new File(args[3] + '.inventory.json').text = JsonOutput.toJson(
        [initial: initialInventory, extraVertex: extraVertexInventory, extraSchema: extraSchemaInventory])
    Set types = [] as Set
    fixture.operations.eachWithIndex { vector, index ->
        def script = vector.operationType >= 1000 ? updates : reads
        script.binding = new Binding([g: g, operationType: vector.operationType, parameters: vector.parameters])
        def observed = script.run()
        if (vector.operationType >= 1000) {
            assert observed instanceof List && observed.size() == 1 && observed[0] instanceof Map
            observed = observed[0]
        }
        assert observed == vector.result : "Operation ${index} / ${vector.operationType}: ${observed} != ${vector.result}"
        graph.tx().commit()
        types.add(vector.operationType)
    }
    // Fail after creating a vertex and several edges; verify the real engine rolled all changes back.
    def addPerson = fixture.operations.find { it.operationType == 1001 }.parameters.clone()
    addPerson.personId = 999999L
    addPerson.tagIds = [999999L]
    updates.binding = new Binding([g: g, operationType: 1001, parameters: addPerson])
    boolean failed = false
    try { updates.run() } catch (IllegalArgumentException expected) { failed = true }
    assert failed && !g.V('person:999999').hasNext()
    graph.tx().rollback()
    new File(args[3]).withWriter('UTF-8') { writer ->
        writer.write(JsonOutput.toJson([scope: 'hugegraph-memory-unit-semantics', operations: types.size(),
            orderedRecords: fixture.operations.size(), rollbackVerified: true, reconciliationScriptVerified: true, inventoryScriptVerified: true, hugegraphExecuted: true,
            hstoreExecuted: false, performanceVerdict: 'not-evaluated']))
    }
} finally {
    graph.close()
    HugeFactory.shutdown(3L, true)
}
