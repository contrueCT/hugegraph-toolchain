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

import org.apache.hugegraph.backend.page.PageInfo
import org.apache.hugegraph.type.HugeType
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator

// No expected counts or labels are sent. Each request verifies the same restore epoch.
def graph = g.getGraph()
def schema = graph.schema()
if (graph.name() != graphName || !schema.getPropertyKeys().any { it.name() == epochMarker }) {
    throw new IllegalStateException('Target graph or restore epoch mismatch')
}
if (category == 'schema') {
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

    graph.tx().rollback()
    return [[graphName: graphName, epochMarker: epochMarker, kind: 'schema',
             schema: [propertyKeys: pks, vertexLabels: vertices, edgeLabels: edges, indexLabels: indexes]]]
}
if (!(category in ['vertices', 'edges']) || pageSize < 1 || pageSize > 100000 || !(page instanceof String)) {
    throw new IllegalArgumentException('Invalid inventory page request')
}
def type = category == 'vertices' ? HugeType.VERTEX : HugeType.EDGE
// Query moved to hugegraph-struct in 1.8; retain compatibility with 1.7.
Class queryClass
try {
    queryClass = Class.forName('org.apache.hugegraph.query.Query')
} catch (ClassNotFoundException ignored) {
    queryClass = Class.forName('org.apache.hugegraph.backend.query.Query')
}
def query = queryClass.getConstructor(HugeType).newInstance(type)
query.capacity(queryClass.NO_CAPACITY)
query.limit(pageSize as long)
query.page(page)
def iterator = category == 'vertices' ? graph.vertices(query) : graph.edges(query)
Map counts = [:]
long scanned = 0L
String nextPage
try {
    while (iterator.hasNext()) {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException('Inventory page interrupted')
        def element = iterator.next()
        scanned = Math.addExact(scanned, 1L)
        if (scanned > pageSize) throw new IllegalStateException('Backend exceeded page limit')
        String label = element.label()
        if (!label.startsWith('~')) counts[label] = Math.addExact(counts.get(label, 0L) as long, 1L)
    }
    nextPage = PageInfo.pageInfo(iterator)
} finally {
    CloseableIterator.closeIterator(iterator)
    graph.tx().rollback()
}
[[graphName: graphName, epochMarker: epochMarker, kind: 'page', category: category, page: page,
  nextPage: nextPage, scanned: scanned.toString(),
  counts: counts.collectEntries { label, count -> [(label): count.toString()] }]]
