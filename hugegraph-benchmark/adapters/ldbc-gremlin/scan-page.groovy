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

// Read-only: no source records, expected counts, challenge or digests are accepted.
import org.apache.hugegraph.backend.page.PageInfo
import org.apache.hugegraph.type.HugeType
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator

def graph = g.getGraph()
if (graph.name() != graphName || !graph.schema().getPropertyKeys().any { it.name() == epochMarker }) {
    throw new IllegalStateException('Target graph or restore epoch mismatch')
}
// Gremlin HTTP decodes integral JSON bindings as Long; direct script bindings may be Integer.
if (!(category in ['vertices', 'edges']) ||
    !(pageSize instanceof Integer || pageSize instanceof Long) || pageSize < 1 || pageSize > 100000 ||
    !(page instanceof String) || page.length() > 32768) {
    throw new IllegalArgumentException('Invalid scan page request')
}
Class queryClass
try { queryClass = Class.forName('org.apache.hugegraph.query.Query') }
catch (ClassNotFoundException ignored) {
    queryClass = Class.forName('org.apache.hugegraph.backend.query.Query')
}
def query = queryClass.getConstructor(HugeType).newInstance(category == 'vertices' ? HugeType.VERTEX : HugeType.EDGE)
query.capacity(queryClass.NO_CAPACITY)
query.limit(pageSize as long)
query.page(page)
def iterator = category == 'vertices' ? graph.vertices(query) : graph.edges(query)
List rows = []
long scanned = 0L, hidden = 0L
String nextPage
try {
    while (iterator.hasNext()) {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException('Scan interrupted')
        def element = iterator.next()
        scanned = Math.addExact(scanned, 1L)
        if (scanned > pageSize) throw new IllegalStateException('Backend exceeded page limit')
        if (element.label().startsWith('~')) { hidden++; continue }
        Map values = [:]
        element.properties().each { property ->
            def value = property.value()
            if (value instanceof Collection) value = value.toList()
            if (values.containsKey(property.key())) throw new IllegalStateException('Duplicate property')
            values[property.key()] = value
        }
        Map row = [label: element.label(), properties: values]
        if (category == 'vertices') row.id = element.id().toString()
        else {
            row.sourceId = element.outVertex().id().toString()
            row.targetId = element.inVertex().id().toString()
        }
        rows.add(row)
    }
    nextPage = PageInfo.pageInfo(iterator)
} finally {
    CloseableIterator.closeIterator(iterator)
    graph.tx().rollback()
}
[[graphName: graphName, epochMarker: epochMarker, kind: 'scan-page', category: category, page: page,
  nextPage: nextPage, scanned: scanned.toString(), hiddenRecords: hidden.toString(), rows: rows]]
