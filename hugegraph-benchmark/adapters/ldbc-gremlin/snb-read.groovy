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

import java.time.Instant
import java.time.ZoneOffset
import org.apache.tinkerpop.gremlin.structure.Direction

// Executed inside the server. IDs and epochs are exact Java longs; all output is consumed by the driver.
// This is a correctness-first adapter. Its query plan/version must be disclosed with performance results.
class SnbTop {
    int limit
    Closure compare
    PriorityQueue<Map> rows
    SnbTop(int limit, Closure compare) {
        if (limit <= 0) throw new IllegalArgumentException('Nonpositive limit')
        this.limit = limit
        this.compare = compare
        this.rows = new PriorityQueue<Map>(Math.min(limit, 1024), { a, b -> compare(b, a) } as Comparator)
    }
    void add(Map row) {
        if (rows.size() < limit) rows.add(row)
        else if (compare(row, rows.peek()) < 0) { rows.poll(); rows.add(row) }
    }
    List<Map> result() { new ArrayList<Map>(rows).sort(compare) }
}

class SnbRead {
    def g
    Map p
    static final String KNOWS = 'person_knows_person'
    static final String[] CREATOR = ['post_hasCreator_person', 'comment_hasCreator_person']
    static final String[] REPLY = ['comment_replyOf_post', 'comment_replyOf_comment']
    SnbRead(g, Map p) { this.g = g; this.p = p }
    static long n(value) {
        if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long ||
              value instanceof BigInteger)) throw new IllegalArgumentException('Expected exact integer')
        if (value instanceof BigInteger) return value.longValueExact()
        value.longValue()
    }
    static Object prop(v, String name) { v.property(name).orElse(null) }
    static long id(v) { n(prop(v, 'ldbcId')) }
    def vertex(String type, value) {
        String prefix = type in ['post', 'comment', 'message'] ? 'message' : type
        def rows = g.V(prefix + ':' + n(value)).toList()
        if (rows.isEmpty()) return null
        def v = rows[0]
        if (type != 'message' && v.label() != type) throw new IllegalStateException('Endpoint type mismatch')
        v
    }
    def firstOut(v, String... labels) {
        def out = g.V(v.id()).out(labels).toList()
        if (out.size() != 1) throw new IllegalStateException('Expected exactly one ' + labels.toList())
        out[0]
    }
    def author(v) { firstOut(v, CREATOR) }
    static Map names(v) { [personId: id(v), personFirstName: prop(v, 'firstName'), personLastName: prop(v, 'lastName')] }
    static Object content(v, boolean imageFirst = false) {
        def preferred = prop(v, imageFirst ? 'imageFile' : 'content')
        preferred != null ? preferred : prop(v, imageFirst ? 'content' : 'imageFile')
    }
    List neighbors(Object vertexId) { g.V(vertexId).both(KNOWS).dedup().toList() }
    Map friends(root, int depth) {
        Map found = [(root.id()): [vertex: root, distance: 0]]
        List frontier = [root]
        for (int distance = 1; distance <= depth && !frontier.isEmpty(); distance++) {
            List next = []
            frontier.each { from -> neighbors(from.id()).each { v ->
                if (!found.containsKey(v.id())) { found[v.id()] = [vertex: v, distance: distance]; next.add(v) }
            } }
            frontier = next
        }
        found.remove(root.id())
        found
    }
    def originalPost(message) {
        Set seen = new HashSet()
        def current = message
        while (current.label() != 'post') {
            if (!seen.add(current.id())) throw new IllegalStateException('Cycle in reply chain')
            current = firstOut(current, REPLY)
        }
        current
    }
    SnbTop top(Closure compare, int defaultLimit = 20) {
        new SnbTop(p.containsKey('limit') ? Math.toIntExact(n(p.limit)) : defaultLimit, compare)
    }
    static Closure recent(String date, String identifier) {
        { a, b -> (b[date] <=> a[date]) ?: (a[identifier] <=> b[identifier]) }
    }
    Map messageRow(m, person) {
        names(person) + [messageId: id(m), messageContent: content(m), messageCreationDate: n(prop(m, 'creationDate'))]
    }
    long endDate() { Math.addExact(n(p.startDate), Math.multiplyExact(n(p.durationDays), 86400000L)) }
    List organizationRows(person, String edgeLabel, String year) {
        List values = []
        g.V(person.id()).outE(edgeLabel).each { edge ->
            def org = edge.inVertex()
            values.add([organizationName: prop(org, 'name'), year: Math.toIntExact(n(prop(edge, year))),
                        placeName: prop(firstOut(org, 'organisation_isLocatedIn_place'), 'name')])
        }
        values
    }
    List collection(person, String key) { g.V(person.id()).values(key).toList().collectMany { v ->
        v instanceof Collection ? new ArrayList(v) : [v]
    } }
    boolean matchingTag(tag, String name) {
        Set seen = new HashSet()
        List todo = [tag]
        while (!todo.isEmpty()) {
            def current = todo.remove(todo.size() - 1)
            if (!seen.add(current.id())) continue
            if (prop(current, 'name') == name) return true
            todo.addAll(g.V(current.id()).out('tag_hasType_tagclass', 'tagclass_isSubclassOf_tagclass').toList())
        }
        false
    }
    // Expand one complete layer of the smaller frontier. Keep predecessor DAGs for all shortest paths.
    Map shortest(left, right, boolean paths) {
        if (left.id() == right.id()) return [distance: 0, paths: [[left.id()]]]
        Map dl = [(left.id()): 0], dr = [(right.id()): 0]
        Map pl = [:], pr = [:]
        Set fl = [left.id()] as Set, fr = [right.id()] as Set
        List meetings = []
        int best = Integer.MAX_VALUE
        while (!fl.isEmpty() && !fr.isEmpty() && meetings.isEmpty()) {
            boolean forward = fl.size() <= fr.size()
            Set frontier = forward ? fl : fr
            Map own = forward ? dl : dr, other = forward ? dr : dl, parents = forward ? pl : pr
            Set next = new LinkedHashSet()
            frontier.each { from -> neighbors(from).each { v ->
                def to = v.id()
                int distance = own[from] + 1
                if (other.containsKey(to)) {
                    int total = distance + other[to]
                    if (total < best) { best = total; meetings.clear() }
                    if (total == best) meetings.add(forward ? [from, to] : [to, from])
                } else if (!own.containsKey(to)) {
                    own[to] = distance
                    if (paths) parents[to] = [from]
                    next.add(to)
                } else if (paths && own[to] == distance) parents[to].add(from)
            } }
            if (forward) fl = next else fr = next
        }
        if (meetings.isEmpty()) return [distance: -1, paths: []]
        if (!paths) return [distance: best, paths: []]
        Closure prefixes
        prefixes = { Object end, Object root, Map parents ->
            if (end == root) return [[root]]
            parents[end].collectMany { predecessor -> prefixes(predecessor, root, parents).collect { it + [end] } }
        }
        List all = []
        meetings.unique().each { pair ->
            prefixes(pair[0], left.id(), pl).each { a -> prefixes(pair[1], right.id(), pr).each { b ->
                all.add(a + b.reverse())
            } }
        }
        [distance: best, paths: all]
    }
    long halfWeight(Object a, Object b) {
        long weight = 0L
        [[a, b], [b, a]].each { pair ->
            g.V(pair[0]).in('comment_hasCreator_person').each { comment ->
                def parent = firstOut(comment, REPLY)
                if (author(parent).id() == pair[1]) weight += parent.label() == 'post' ? 2L : 1L
            }
        }
        weight
    }
    List execute(int type) {
        if (type in [13, 14]) {
            def a = vertex('person', p.person1Id), b = vertex('person', p.person2Id)
            if (a == null || b == null) throw new IllegalArgumentException('Missing path endpoint')
            def found = shortest(a, b, type == 14)
            if (type == 13) return [[shortestPathLength: found.distance]]
            Map weights = [:]
            return found.paths.collect { path ->
                long half = 0L
                for (int i = 1; i < path.size(); i++) {
                    def pair = [path[i - 1], path[i]].sort()
                    if (!weights.containsKey(pair)) weights[pair] = halfWeight(pair[0], pair[1])
                    half = Math.addExact(half, (long) weights[pair])
                }
                [personIdsInPath: path.collect { id(g.V(it).next()) }, pathWeight: half / 2.0d]
            }.sort { aRow, bRow -> bRow.pathWeight <=> aRow.pathWeight }
        }
        boolean messageInput = type in [104, 105, 106, 107]
        def root = vertex(messageInput ? 'message' : 'person', messageInput ? p.messageId : p.personId)
        if (root == null) return []
        switch (type) {
            case 1:
                def answer = top { a, b -> (a.distance <=> b.distance) ?: (a.lastName <=> b.lastName) ?: (a.id <=> b.id) }
                friends(root, 3).values().each { f ->
                    if (prop(f.vertex, 'firstName') == p.firstName)
                        answer.add([vertex: f.vertex, distance: f.distance, lastName: prop(f.vertex, 'lastName'), id: id(f.vertex)])
                }
                return answer.result().collect { f ->
                    def v = f.vertex
                    [friendId: id(v), friendLastName: prop(v, 'lastName'), distanceFromPerson: f.distance,
                     friendBirthday: n(prop(v, 'birthday')), friendCreationDate: n(prop(v, 'creationDate')),
                     friendGender: prop(v, 'gender'), friendBrowserUsed: prop(v, 'browserUsed'),
                     friendLocationIp: prop(v, 'locationIP'), friendEmails: collection(v, 'emails'),
                     friendLanguages: collection(v, 'languages'),
                     friendCityName: prop(firstOut(v, 'person_isLocatedIn_place'), 'name'),
                     friendUniversities: organizationRows(v, 'person_studyAt_organisation', 'classYear'),
                     friendCompanies: organizationRows(v, 'person_workAt_organisation', 'workFrom')]
                }
            case 2:
            case 9:
                def answer = top(recent('messageCreationDate', 'messageId'))
                friends(root, type == 2 ? 1 : 2).values().each { f ->
                    g.V(f.vertex.id()).in(CREATOR).each { m ->
                        long date = n(prop(m, 'creationDate'))
                        if (type == 2 ? date <= n(p.maxDate) : date < n(p.maxDate)) answer.add(messageRow(m, f.vertex))
                    }
                }
                return answer.result()
            case 3:
                def answer = top { a, b -> (b.count <=> a.count) ?: (a.personId <=> b.personId) }
                long start = n(p.startDate), end = endDate()
                friends(root, 2).values().each { f ->
                    def city = firstOut(f.vertex, 'person_isLocatedIn_place')
                    def country = prop(firstOut(city, 'place_isPartOf_place'), 'name')
                    if (!(country in [p.countryXName, p.countryYName])) {
                        long x = 0L, y = 0L
                        g.V(f.vertex.id()).in(CREATOR).each { m ->
                            long date = n(prop(m, 'creationDate'))
                            if (date >= start && date < end) {
                                String location = prop(firstOut(m, m.label() + '_isLocatedIn_place'), 'name')
                                if (location == p.countryXName) x++
                                if (location == p.countryYName) y++
                            }
                        }
                        if (x > 0 && y > 0) answer.add(names(f.vertex) + [xCount: x, yCount: y, count: x + y])
                    }
                }
                return answer.result()
            case 4:
                Map counts = [:]; Set old = new HashSet()
                long start = n(p.startDate), end = endDate()
                friends(root, 1).values().each { f ->
                    g.V(f.vertex.id()).in('post_hasCreator_person').dedup().each { post ->
                        long date = n(prop(post, 'creationDate'))
                        g.V(post.id()).out('post_hasTag_tag').dedup().each { tag ->
                            def key = tag.id()
                            if (date < start) old.add(key)
                            if (date >= start && date < end) {
                                if (!counts.containsKey(key)) counts[key] = [tagName: prop(tag, 'name'), postCount: 0]
                                counts[key].postCount++
                            }
                        }
                    }
                }
                def answer = top({ a, b -> (b.postCount <=> a.postCount) ?: (a.tagName <=> b.tagName) }, 10)
                counts.each { key, value -> if (!old.contains(key)) answer.add(value) }
                return answer.result()
            case 5:
                Map forums = [:]
                friends(root, 2).values().each { f ->
                    g.V(f.vertex.id()).inE('forum_hasMember_person').each { edge ->
                        if (n(prop(edge, 'joinDate')) > n(p.minDate)) {
                            def forum = edge.outVertex()
                            if (!forums.containsKey(forum.id())) forums[forum.id()] = [vertex: forum, members: new HashSet()]
                            forums[forum.id()].members.add(f.vertex.id())
                        }
                    }
                }
                def answer = top { a, b -> (b.postCount <=> a.postCount) ?: (a.forumId <=> b.forumId) }
                forums.values().each { f ->
                    long count = 0L
                    g.V(f.vertex.id()).out('forum_containerOf_post').each { post ->
                        if (f.members.contains(author(post).id())) count++
                    }
                    answer.add([forumId: id(f.vertex), forumTitle: prop(f.vertex, 'title'), postCount: Math.toIntExact(count)])
                }
                return answer.result().collect { [forumTitle: it.forumTitle, postCount: it.postCount] }
            case 6:
                Map counts = [:]
                friends(root, 2).values().each { f -> g.V(f.vertex.id()).in('post_hasCreator_person').each { post ->
                    List tags = g.V(post.id()).out('post_hasTag_tag').toList()
                    if (tags.any { prop(it, 'name') == p.tagName }) tags.each { tag ->
                        String name = prop(tag, 'name')
                        if (name != p.tagName) counts[name] = (counts[name] ?: 0) + 1
                    }
                } }
                def answer = top({ a, b -> (b.postCount <=> a.postCount) ?: (a.tagName <=> b.tagName) }, 10)
                counts.each { name, count -> answer.add([tagName: name, postCount: count]) }
                return answer.result()
            case 7:
                Map latest = [:]
                Set known = friends(root, 1).keySet()
                g.V(root.id()).in(CREATOR).each { message ->
                    g.V(message.id()).inE('person_likes_post', 'person_likes_comment').each { edge ->
                        def person = edge.outVertex()
                        long date = n(prop(edge, 'creationDate'))
                        def previous = latest[person.id()]
                        if (previous == null || date > previous.date || (date == previous.date && id(message) < id(previous.message)))
                            latest[person.id()] = [person: person, message: message, date: date]
                    }
                }
                def answer = top(recent('likeCreationDate', 'personId'))
                latest.values().each { l ->
                    long delta = Math.subtractExact((long) l.date, n(prop(l.message, 'creationDate')))
                    answer.add(names(l.person) + [likeCreationDate: l.date, messageId: id(l.message),
                        messageContent: content(l.message), minutesLatency: Math.toIntExact((long) delta.intdiv(60000L)),
                        isNew: !known.contains(l.person.id())])
                }
                return answer.result()
            case 8:
                def answer = top(recent('commentCreationDate', 'commentId'))
                g.V(root.id()).in(CREATOR).in(REPLY).each { comment ->
                    answer.add(names(author(comment)) + [commentCreationDate: n(prop(comment, 'creationDate')),
                               commentId: id(comment), commentContent: prop(comment, 'content')])
                }
                return answer.result()
            case 10:
                Set interests = g.V(root.id()).out('person_hasInterest_tag').id().toSet()
                def answer = top({ a, b -> (b.commonInterestScore <=> a.commonInterestScore) ?: (a.personId <=> b.personId) }, 10)
                friends(root, 2).values().findAll { it.distance == 2 }.each { f ->
                    def birthday = Instant.ofEpochMilli(n(prop(f.vertex, 'birthday'))).atZone(ZoneOffset.UTC)
                    int month = Math.toIntExact(n(p.month))
                    if ((birthday.monthValue == month && birthday.dayOfMonth >= 21) ||
                        (birthday.monthValue == month % 12 + 1 && birthday.dayOfMonth < 22)) {
                        long score = 0L
                        g.V(f.vertex.id()).in('post_hasCreator_person').each { post ->
                            boolean common = g.V(post.id()).out('post_hasTag_tag').id().toList().any { interests.contains(it) }
                            score += common ? 1L : -1L
                        }
                        answer.add(names(f.vertex) + [commonInterestScore: Math.toIntExact(score), personGender: prop(f.vertex, 'gender'),
                            personCityName: prop(firstOut(f.vertex, 'person_isLocatedIn_place'), 'name')])
                    }
                }
                return answer.result()
            case 11:
                def answer = top({ a, b -> (a.organizationWorkFromYear <=> b.organizationWorkFromYear) ?:
                           (a.personId <=> b.personId) ?: (b.organizationName <=> a.organizationName) }, 10)
                friends(root, 2).values().each { f -> g.V(f.vertex.id()).outE('person_workAt_organisation').each { edge ->
                    def company = edge.inVertex()
                    long year = n(prop(edge, 'workFrom'))
                    if (year < n(p.workFromYear) && prop(firstOut(company, 'organisation_isLocatedIn_place'), 'name') == p.countryName)
                        answer.add(names(f.vertex) + [organizationName: prop(company, 'name'), organizationWorkFromYear: Math.toIntExact(year)])
                } }
                return answer.result()
            case 12:
                def answer = top { a, b -> (b.replyCount <=> a.replyCount) ?: (a.personId <=> b.personId) }
                Map tagMatches = [:]
                friends(root, 1).values().each { f ->
                    Set tags = new HashSet(), comments = new HashSet()
                    g.V(f.vertex.id()).in('comment_hasCreator_person').each { comment ->
                        g.V(comment.id()).out('comment_replyOf_post').out('post_hasTag_tag').each { tag ->
                            if (!tagMatches.containsKey(tag.id())) tagMatches[tag.id()] = matchingTag(tag, p.tagClassName)
                            if (tagMatches[tag.id()]) { tags.add(prop(tag, 'name')); comments.add(comment.id()) }
                        }
                    }
                    if (!comments.isEmpty()) answer.add(names(f.vertex) + [tagNames: tags.toList(), replyCount: comments.size()])
                }
                return answer.result()
            case 101:
                return [[firstName: prop(root, 'firstName'), lastName: prop(root, 'lastName'),
                         birthday: n(prop(root, 'birthday')), locationIp: prop(root, 'locationIP'),
                         browserUsed: prop(root, 'browserUsed'), cityId: id(firstOut(root, 'person_isLocatedIn_place')),
                         gender: prop(root, 'gender'), creationDate: n(prop(root, 'creationDate'))]]
            case 102:
                def answer = top(recent('messageCreationDate', 'messageId'), 10)
                g.V(root.id()).in(CREATOR).each { message ->
                    answer.add([vertex: message, messageId: id(message), messageCreationDate: n(prop(message, 'creationDate'))])
                }
                return answer.result().collect { row ->
                    def original = originalPost(row.vertex), person = author(original)
                    [messageId: row.messageId, messageContent: content(row.vertex, true),
                     messageCreationDate: row.messageCreationDate, originalPostId: id(original),
                     originalPostAuthorId: id(person), originalPostAuthorFirstName: prop(person, 'firstName'),
                     originalPostAuthorLastName: prop(person, 'lastName')]
                }
            case 103:
                return g.V(root.id()).bothE(KNOWS).toList().collect { edge ->
                    def friend = edge.outVertex().id() == root.id() ? edge.inVertex() : edge.outVertex()
                    [personId: id(friend), firstName: prop(friend, 'firstName'), lastName: prop(friend, 'lastName'),
                     friendshipCreationDate: n(prop(edge, 'creationDate'))]
                }.sort(recent('friendshipCreationDate', 'personId'))
            case 104:
                return [[messageContent: content(root), messageCreationDate: n(prop(root, 'creationDate'))]]
            case 105:
                def person = author(root)
                return [[personId: id(person), firstName: prop(person, 'firstName'), lastName: prop(person, 'lastName')]]
            case 106:
                def post = originalPost(root)
                def forums = g.V(post.id()).in('forum_containerOf_post').toList()
                if (forums.size() != 1) throw new IllegalStateException('Expected exactly one original forum')
                def moderator = firstOut(forums[0], 'forum_hasModerator_person')
                return [[forumId: id(forums[0]), forumTitle: prop(forums[0], 'title'), moderatorId: id(moderator),
                         moderatorFirstName: prop(moderator, 'firstName'), moderatorLastName: prop(moderator, 'lastName')]]
            case 107:
                Set known = neighbors(author(root).id()).collect { it.id() } as Set
                return g.V(root.id()).in(REPLY).toList().collect { comment ->
                    def person = author(comment)
                    [commentId: id(comment), commentContent: prop(comment, 'content'),
                     commentCreationDate: n(prop(comment, 'creationDate')), replyAuthorId: id(person),
                     replyAuthorFirstName: prop(person, 'firstName'), replyAuthorLastName: prop(person, 'lastName'),
                     isReplyAuthorKnowsOriginalMessageAuthor: known.contains(person.id())]
                }.sort(recent('commentCreationDate', 'replyAuthorId'))
            default:
                throw new IllegalArgumentException('Unsupported read operation: ' + type)
        }
    }
}
new SnbRead(g, parameters).execute(operationType as int)
