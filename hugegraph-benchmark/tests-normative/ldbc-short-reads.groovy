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

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

// Hand-authored semantic unit graph; never a performance dataset or an SF100 substitute.
def templates = new JsonSlurper().parse(new File(args[0]))
def graph = TinkerGraph.open()
def vertex = { String label, long id, Map properties ->
    def prefix = label in ['post', 'comment'] ? 'message' : label
    def v = graph.addVertex(T.id, "${prefix}:${id}".toString(), T.label, label)
    v.property('ldbcId', id)
    properties.each { k, value -> v.property(k, value) }
    v
}
def place = vertex('place', 99L, [name: '北京'])
def person = { long id, String first ->
    def p = vertex('person', id, [firstName: first, lastName: '姓', birthday: 10L,
                   locationIP: '127.0.0.1', browserUsed: 'browser', gender: 'gender', creationDate: 20L])
    p.addEdge('person_isLocatedIn_place', place)
    p
}
def p1 = person(1L, '一')
def p2 = person(2L, '二')
def p3 = person(3L, '三')
def p4 = person(4L, '四')
def big = person(9007199254740993L, '大')
p2.addEdge('person_knows_person', p1, 'creationDate', 30L)
p1.addEdge('person_knows_person', p3, 'creationDate', 30L)
big.addEdge('person_knows_person', p1, 'creationDate', 10L)
def post = vertex('post', 10L, [content: '正文 | 文本', creationDate: 100L])
def image = vertex('post', 11L, [imageFile: '图片.png', content: '优先级测试', creationDate: 200L])
post.addEdge('post_hasCreator_person', p1)
image.addEdge('post_hasCreator_person', p1)
def largeMessage = vertex('post', 9007199254740995L, [imageFile: '大.png', creationDate: 250L])
largeMessage.addEdge('post_hasCreator_person', big)
def comment = { long id, long date, who, parent ->
    def c = vertex('comment', id, [content: "评论${id}".toString(), creationDate: date])
    c.addEdge('comment_hasCreator_person', who)
    c.addEdge(parent.label() == 'post' ? 'comment_replyOf_post' : 'comment_replyOf_comment', parent)
    c
}
def c20 = comment(20L, 300L, p1, post)
def c21 = comment(21L, 400L, p1, c20)
comment(22L, 190L, p2, post)
comment(23L, 190L, p3, post)
comment(24L, 190L, p4, post)
def forum = vertex('forum', 101L, [title: '论坛'])
forum.addEdge('forum_containerOf_post', post)
forum.addEdge('forum_hasModerator_person', p3)
def run = { String id, String input, int limit = 10 ->
    def bindings = new Binding([g: graph.traversal(), __: __, asc: Order.asc, desc: Order.desc,
                               vertexId: templates[id].prefix + input, queryLimit: limit,
                               eq: { value -> P.eq(value) }])
    def shell = new GroovyShell(bindings)
    shell.evaluate(templates[id].gremlin).toList()
}
def cases = []
def check = { String name, Closure action -> action(); cases << name }
check('IS1 complete profile and location') {
    assert run('IS1', '1') == [[firstName: '一', lastName: '姓', birthday: 10L,
             locationIp: '127.0.0.1', browserUsed: 'browser', cityId: 99L, gender: 'gender', creationDate: 20L]]
}
check('IS2 nested ancestor, global output order, limit and projection') {
    def rows = run('IS2', '1')
    assert rows*.messageId == [21L, 20L, 11L, 10L]
    assert rows[0] == [messageId: 21L, messageContent: '评论21', messageCreationDate: 400L,
            originalPostId: 10L, originalPostAuthorId: 1L, originalPostAuthorFirstName: '一', originalPostAuthorLastName: '姓']
    assert rows[2].messageContent == '图片.png'
    assert rows[2].originalPostId == 11L
    assert run('IS2', '1', 2) == rows.take(2)
}
check('IS3 incoming/outgoing friendship and stable tie order') {
    def rows = run('IS3', '1')
    assert rows*.personId == [2L, 3L, 9007199254740993L]
    assert rows*.friendshipCreationDate == [30L, 30L, 10L]
    assert rows[0] == [personId: 2L, firstName: '二', lastName: '姓', friendshipCreationDate: 30L]
    assert run('IS3', '4') == []
}
check('IS4 content preference, image fallback and comment') {
    assert run('IS4', '10') == [[messageContent: '正文 | 文本', messageCreationDate: 100L]]
    assert run('IS4', '11') == [[messageContent: '优先级测试', messageCreationDate: 200L]]
    assert run('IS4', '9007199254740995') == [[messageContent: '大.png', messageCreationDate: 250L]]
    assert run('IS4', '21') == [[messageContent: '评论21', messageCreationDate: 400L]]
}
check('IS5 post/comment creator and exact long ID') {
    assert run('IS5', '9007199254740995') == [[personId: 9007199254740993L, firstName: '大', lastName: '姓']]
    assert run('IS5', '22') == [[personId: 2L, firstName: '二', lastName: '姓']]
}
check('IS6 zero-hop post and nested comment use same original forum') {
    def expected = [[forumId: 101L, forumTitle: '论坛', moderatorId: 3L,
                     moderatorFirstName: '三', moderatorLastName: '姓']]
    assert run('IS6', '10') == expected
    assert run('IS6', '21') == expected
}
check('IS7 direct replies, ordered author ties and friendship in either direction') {
    def rows = run('IS7', '10')
    assert rows*.commentId == [20L, 22L, 23L, 24L]
    assert rows*.isReplyAuthorKnowsOriginalMessageAuthor == [false, true, true, false]
    assert rows[1] == [commentId: 22L, commentContent: '评论22', commentCreationDate: 190L,
            replyAuthorId: 2L, replyAuthorFirstName: '二', replyAuthorLastName: '姓',
            isReplyAuthorKnowsOriginalMessageAuthor: true]
    assert run('IS7', '20')*.commentId == [21L]
    assert run('IS7', '21') == []
}
check('Absent IDs never manufacture projected rows') {
    templates.keySet().each { id -> assert run(id, '9223372036854775807') == [] }
}
graph.close()
println JsonOutput.toJson([scope: 'tinkergraph-short-read-semantics', cases: cases, passed: cases.size(),
                          operations: 7, hugegraphExecuted: false, performanceVerdict: 'not-evaluated'])
