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

// Every requested identity is observed independently; no expected property values are sent.
def graph = g.getGraph()
// Gremlin statically imports T.label; resolve this request binding explicitly.
def selectedLabel = binding.getVariable('label')
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
    else { row.sourceId = element.outVertex().id().toString(); row.targetId = element.inVertex().id().toString() }
    row
}
def rows = selectors.collect { selector ->
    def elements = category == 'vertices' ? g.V(selector.id).toList() :
        g.V(selector.sourceId).outE(selectedLabel).has('edgeKey', selector.edgeKey).toList()
    elements.collect(snapshot)
}
// Close the read transaction before the next batch; the externally held lease forbids writes.
graph.tx().rollback()
[[graphName: graphName, epochMarker: epochMarker, rows: rows]]
