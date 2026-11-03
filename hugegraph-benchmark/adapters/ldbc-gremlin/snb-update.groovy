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

import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.VertexProperty

class SnbUpdate {
    def g
    Map p
    int edgeOrdinal = 0
    SnbUpdate(g, Map p) { this.g = g; this.p = p }
    static long n(value) {
        if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long ||
              value instanceof BigInteger)) throw new IllegalArgumentException('Expected exact integer')
        if (value instanceof BigInteger) return value.longValueExact()
        value.longValue()
    }
    static String graphId(String type, value) {
        long number = n(value)
        if (number < 0) throw new IllegalArgumentException('Negative entity ID')
        (type in ['post', 'comment'] ? 'message' : type) + ':' + number
    }
    def existing(String type, value) {
        def vertices = g.V(graphId(type, value)).toList()
        if (vertices.size() != 1 || vertices[0].label() != type)
            throw new IllegalArgumentException('Missing or mistyped ' + type + ' endpoint')
        vertices[0]
    }
    def create(String type, value, Map fields) {
        String identifier = graphId(type, value)
        if (g.V(identifier).hasNext()) throw new IllegalStateException('Duplicate update vertex')
        def vertex = g.getGraph().addVertex(T.id, identifier, T.label, type, 'ldbcId', n(value))
        fields.each { name, field ->
            if (field != null && field != '') vertex.property(name, field)
        }
        vertex
    }
    void relation(from, String label, to, Map properties = [:], boolean allowParallel = false) {
        if (!allowParallel && g.V(from.id()).outE(label).toList().any { it.inVertex().id() == to.id() })
            throw new IllegalStateException('Duplicate update relationship')
        // Initial import uses positive edgeKey values. Updates use negative, per-operation ordinals.
        // Every update either creates a new vertex or a unique logical endpoint pair; no hidden retries/upserts.
        long key = -(++edgeOrdinal as long)
        List args = ['edgeKey', key]
        properties.each { name, value -> args.add(name); args.add(value) }
        from.addEdge(label, to, args.toArray())
    }
    void tags(vertex, String edgeLabel) {
        if (!(p.tagIds instanceof List)) throw new IllegalArgumentException('tagIds must be a list')
        Set seen = new HashSet()
        p.tagIds.each { tagId ->
            if (!seen.add(n(tagId))) throw new IllegalArgumentException('Duplicate tag ID in update')
            relation(vertex, edgeLabel, existing('tag', tagId))
        }
    }
    void organizations(person, String field, String label, String year) {
        if (!(p[field] instanceof List)) throw new IllegalArgumentException('Expected organization list')
        p[field].each { entry ->
            if (!(entry instanceof Map) || entry.keySet() != (['organizationId', 'year'] as Set))
                throw new IllegalArgumentException('Expected organizationId/year')
            relation(person, label, existing('organisation', entry.organizationId), [(year): n(entry.year)], true)
        }
    }
    void apply(int type) {
        switch (type) {
            case 1001:
                def city = existing('place', p.cityId)
                def person = create('person', p.personId, [firstName: p.personFirstName, lastName: p.personLastName,
                    gender: p.gender, birthday: n(p.birthday), creationDate: n(p.creationDate),
                    locationIP: p.locationIP, browserUsed: p.browserUsed])
                for (String field : ['languages', 'emails']) {
                    if (!(p[field] instanceof List) || p[field].any { !(it instanceof String) })
                        throw new IllegalArgumentException('Expected string list for ' + field)
                    p[field].each { value -> person.property(VertexProperty.Cardinality.set, field, value) }
                }
                relation(person, 'person_isLocatedIn_place', city)
                tags(person, 'person_hasInterest_tag')
                organizations(person, 'studyAt', 'person_studyAt_organisation', 'classYear')
                organizations(person, 'workAt', 'person_workAt_organisation', 'workFrom')
                break
            case 1002:
                relation(existing('person', p.personId), 'person_likes_post', existing('post', p.postId),
                         [creationDate: n(p.creationDate)])
                break
            case 1003:
                relation(existing('person', p.personId), 'person_likes_comment', existing('comment', p.commentId),
                         [creationDate: n(p.creationDate)])
                break
            case 1004:
                def moderator = existing('person', p.moderatorPersonId)
                def forum = create('forum', p.forumId, [title: p.forumTitle, creationDate: n(p.creationDate)])
                relation(forum, 'forum_hasModerator_person', moderator)
                tags(forum, 'forum_hasTag_tag')
                break
            case 1005:
                relation(existing('forum', p.forumId), 'forum_hasMember_person', existing('person', p.personId),
                         [joinDate: n(p.joinDate)])
                break
            case 1006:
                def author = existing('person', p.authorPersonId), country = existing('place', p.countryId)
                def forum = existing('forum', p.forumId)
                def post = create('post', p.postId, [imageFile: p.imageFile, creationDate: n(p.creationDate),
                    locationIP: p.locationIP, browserUsed: p.browserUsed, language: p.language,
                    content: p.content, length: n(p.length)])
                relation(post, 'post_hasCreator_person', author)
                relation(post, 'post_isLocatedIn_place', country)
                relation(forum, 'forum_containerOf_post', post)
                tags(post, 'post_hasTag_tag')
                break
            case 1007:
                long postId = n(p.replyToPostId), commentId = n(p.replyToCommentId)
                if (!((postId >= 0 && commentId == -1L) || (commentId >= 0 && postId == -1L)))
                    throw new IllegalArgumentException('Exactly one reply target is required; the other must be -1')
                def parent = existing(postId >= 0 ? 'post' : 'comment', postId >= 0 ? postId : commentId)
                def author = existing('person', p.authorPersonId), country = existing('place', p.countryId)
                def comment = create('comment', p.commentId, [creationDate: n(p.creationDate), locationIP: p.locationIP,
                    browserUsed: p.browserUsed, content: p.content, length: n(p.length)])
                relation(comment, 'comment_hasCreator_person', author)
                relation(comment, 'comment_isLocatedIn_place', country)
                relation(comment, 'comment_replyOf_' + parent.label(), parent)
                tags(comment, 'comment_hasTag_tag')
                break
            case 1008:
                def first = existing('person', p.person1Id), second = existing('person', p.person2Id)
                if (first.id() == second.id() || g.V(first.id()).both('person_knows_person').toList().any { it.id() == second.id() })
                    throw new IllegalStateException('Self/duplicate friendship')
                relation(first, 'person_knows_person', second, [creationDate: n(p.creationDate)])
                break
            default:
                throw new IllegalArgumentException('Unsupported update operation: ' + type)
        }
    }
    Map execute(int type) {
        def graph = g.getGraph()
        if (!graph.features().graph().supportsTransactions())
            throw new UnsupportedOperationException('Updates require transactional backend support')
        try {
            apply(type)
            graph.tx().commit()
            [operationType: type, committed: true, relationshipsCreated: edgeOrdinal]
        } catch (Throwable failure) {
            try { graph.tx().rollback() } catch (Throwable rollbackFailure) { failure.addSuppressed(rollbackFailure) }
            throw failure
        }
    }
}
// Gremlin HTTP expands a top-level Map into entries; return one result row explicitly.
[new SnbUpdate(g, parameters).execute(operationType as int)]
