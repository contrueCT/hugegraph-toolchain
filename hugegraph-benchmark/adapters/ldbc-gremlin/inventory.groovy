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

import org.apache.hugegraph.type.HugeType
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator

// Read-only initial-state inventory. No expected counts or schema values are sent to the server.
def graph = g.getGraph()
def schema = graph.schema()
if (graph.name() != graphName || !schema.getPropertyKeys().any { it.name() == epochMarker }) {
    throw new IllegalStateException('Target graph or restore epoch mismatch')
}
def names = { identifiers -> identifiers.collect { graph.propertyKey(it).name() } }
def common = { element ->
    [properties: names(element.properties()).sort(), nullableKeys: names(element.nullableKeys()).sort(),
     enableLabelIndex: element.enableLabelIndex(), ttl: element.ttl().toString(),
     ttlStartTime: element.ttlStartTime().asLong() == 0L ? null : graph.propertyKey(element.ttlStartTime()).name(),
     status: element.status().name()]
}
def pks = schema.getPropertyKeys().findAll { it.name() != epochMarker }.collectEntries { pk ->
    [(pk.name()): [dataType: pk.dataType().name(), cardinality: pk.cardinality().name(),
        aggregateType: pk.aggregateType().name(), writeType: pk.writeType().name(), status: pk.status().name()]]
}
def vertices = schema.getVertexLabels().collectEntries { vl ->
    [(vl.name()): common(vl) + [idStrategy: vl.idStrategy().name(), primaryKeys: names(vl.primaryKeys())]]
}
def edges = schema.getEdgeLabels().collectEntries { el ->
    [(el.name()): common(el) + [frequency: el.frequency().name(), edgeLabelType: el.edgeLabelType().name(),
        sourceLabel: graph.vertexLabel(el.sourceLabel()).name(), targetLabel: graph.vertexLabel(el.targetLabel()).name(),
        sortKeys: names(el.sortKeys())]]
}
def indexes = schema.getIndexLabels().collectEntries { index ->
    String base = index.baseType().name() == 'VERTEX_LABEL' ? graph.vertexLabel(index.baseValue()).name() :
                                                           graph.edgeLabel(index.baseValue()).name()
    [(index.name()): [baseType: index.baseType().name(), baseLabel: base, indexType: index.indexType().name(),
                      fields: names(index.indexFields()), status: index.status().name()]]
}
// Query moved to hugegraph-struct in 1.8; retain compatibility with 1.7.
Class queryClass
try {
    queryClass = Class.forName('org.apache.hugegraph.query.Query')
} catch (ClassNotFoundException ignored) {
    queryClass = Class.forName('org.apache.hugegraph.backend.query.Query')
}
def exactCounts = { type, labels ->
    // Scan each element once. Label-pushdown/aggregate fast paths are not used as the acceptance oracle.
    def query = queryClass.getConstructor(HugeType).newInstance(type)
    query.capacity(queryClass.NO_CAPACITY)
    query.limit(queryClass.NO_LIMIT)
    def iterator = type == HugeType.VERTEX ? graph.vertices(query) : graph.edges(query)
    Map counts = labels.collectEntries { label -> [(label): 0L] }
    try {
        while (iterator.hasNext()) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException('Inventory scan interrupted')
            def element = iterator.next()
            String label = element.label()
            if (label.startsWith('~')) continue
            counts[label] = Math.addExact(counts.get(label, 0L) as long, 1L)
        }
    } finally {
        CloseableIterator.closeIterator(iterator)
    }
    counts.collectEntries { label, count -> [(label): count.toString()] }
}
// HugeGraph's public schema API excludes internal hidden system labels; retain this scope in the evidence.
def counts = [vertices: exactCounts(HugeType.VERTEX, vertices.keySet()),
              edges: exactCounts(HugeType.EDGE, edges.keySet())]
graph.tx().rollback()
[[graphName: graphName, epochMarker: epochMarker, inventory: [schemaVersion: 1, scope: 'public-ldbc-initial-graph',
    schema: [propertyKeys: pks, vertexLabels: vertices, edgeLabels: edges, indexLabels: indexes], counts: counts]]]
