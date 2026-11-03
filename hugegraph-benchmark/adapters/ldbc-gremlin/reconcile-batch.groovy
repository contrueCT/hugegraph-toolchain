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

// HugeGraph 1.8 ID codecs; every source record is still compared independently by Python.
def graph = g.getGraph()
// Gremlin statically imports T.label; resolve the request binding explicitly.
def selectedLabel = binding.getVariable('label')
try {
    if (graph.name() != graphName ||
        !graph.schema().getPropertyKeys().any { it.name() == epochMarker }) {
        throw new IllegalStateException('Target graph or restore epoch mismatch')
    }
    def snapshot = { element ->
        Map values = [:]
        element.properties().each { property ->
            def value = property.value()
            if (value instanceof Collection) value = value.toList()
            if (values.containsKey(property.key())) {
                throw new IllegalStateException('Unexpected duplicate property')
            }
            values[property.key()] = value
        }
        Map row = [label: element.label(), properties: values]
        if (category == 'vertices') row.id = element.id().toString()
        else {
            row.sourceId = element.outVertex().id().toString()
            row.targetId = element.inVertex().id().toString()
        }
        row
    }
    def identities
    if (category == 'vertices') {
        identities = selectors.collect { it.id }
    } else if (category == 'edges') {
        def edgeLabel = graph.edgeLabel(selectedLabel)
        def key = graph.propertyKey('edgeKey')
        if (edgeLabel.hasFather() || edgeLabel.sortKeys() != [key.id()]) {
            throw new IllegalStateException('Expected standalone edge label with edgeKey sort key')
        }
        identities = selectors.collect { selector ->
            def sortValue = org.apache.hugegraph.query.ConditionQuery.concatValues(
                [key.serialValue(selector.edgeKey, true)])
            new org.apache.hugegraph.id.EdgeId(
                org.apache.hugegraph.id.IdGenerator.of(selector.sourceId),
                org.apache.hugegraph.type.define.Directions.OUT,
                edgeLabel.id(), edgeLabel.id(), sortValue,
                org.apache.hugegraph.id.IdGenerator.of(selector.targetId))
        }
    } else {
        throw new IllegalArgumentException('Unexpected record category')
    }
    def requested = identities.collect { it.toString() }.toSet()
    def found = []
    if (!identities.isEmpty()) {
        found = category == 'vertices' ? g.V(identities.toArray()).toList() : g.E(identities.toArray()).toList()
    }
    def byId = [:]
    found.each { element ->
        def id = element.id().toString()
        if (!requested.contains(id)) throw new IllegalStateException('Unrequested ID observation')
        def observed = snapshot(element)
        if (!byId.containsKey(id)) byId[id] = [observed]
        // Repeated selectors may return the same element more than once.
        else if (byId[id][0] != observed) throw new IllegalStateException('Conflicting duplicate ID observation')
    }
    def rows = identities.collect { byId[it.toString()] ?: [] }
    return [[graphName: graphName, epochMarker: epochMarker, rows: rows]]
} finally {
    // The external lease excludes writers; close the read transaction on success and failure.
    graph.tx().rollback()
}
