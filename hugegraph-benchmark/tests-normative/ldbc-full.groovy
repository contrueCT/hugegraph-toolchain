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
import java.time.Instant
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.VertexProperty

// Independent hand-computed answer vectors, plus a separate single-frontier path oracle.
// These bounded unit fixtures exercise code only; no performance/SF100 claims are made.
def loader = new GroovyClassLoader()
def readClass = loader.parseClass(new File(args[0]))
def updateClass = loader.parseClass(new File(args[1]))
def graph = TinkerTransactionGraph.open()
def g = graph.traversal()
Set covered = new TreeSet()
List cases = []
List vectors = []
boolean captureVectors = true
def run = { int type, Map params ->
    def script = (type >= 1000 ? updateClass : readClass).newInstance()
    script.binding = new Binding([g: g, operationType: type, parameters: params])
    def result = script.run()
    if (type >= 1000) {
        assert result instanceof List && result.size() == 1 && result[0] instanceof Map
        result = result[0]
    }
    covered.add(type)
    if (captureVectors) vectors.add([operationType: type, parameters: params, result: result])
    result
}
def check = { String name, Closure action -> action(); cases.add(name) }
def fail = { Closure action ->
    boolean rejected = false
    try { action() } catch (IllegalArgumentException | IllegalStateException expected) { rejected = true }
    assert rejected
}
def vertex = { String label, long id, Map props = [:] ->
    String prefix = label in ['post','comment'] ? 'message' : label
    def v = graph.addVertex(T.id, prefix + ':' + id, T.label, label, 'ldbcId', id)
    props.each { k, value -> v.property(k, value) }
    v
}
long ordinal = 0L
def edge = { a, String label, b, Map props = [:] ->
    List values = ['edgeKey', ++ordinal]
    props.each { k, value -> values.add(k); values.add(value) }
    a.addEdge(label, b, values.toArray())
}
def x = vertex('place',100,[name:'X',type:'country']), y = vertex('place',200,[name:'Y',type:'country'])
def z = vertex('place',300,[name:'Z',type:'country'])
def cities = [:]
[[11,x],[22,y],[33,z]].each { pair ->
    cities[pair[0]] = vertex('place',pair[0],[name:'City'+pair[0],type:'city'])
    edge(cities[pair[0]],'place_isPartOf_place',pair[1])
}
def people = [:]
[1L,2L,3L,4L,5L,6L,7L,8L,9007199254740993L].each { id ->
    String birthday = id == 8 ? '1980-01-21T00:00:00Z' : id == 9007199254740993L ? '1980-06-22T00:00:00Z' : '1980-05-21T00:00:00Z'
    String last = [2L:'B',3L:'A',4L:'C',8L:'D',9007199254740993L:'E'].get(id,'Other')
    String first = id in [2L,3L,4L,7L,8L,9007199254740993L] ? 'Same' : 'Root'
    people[id] = vertex('person',id,[firstName:first,lastName:last,birthday:Instant.parse(birthday).toEpochMilli(),
                    creationDate:1L,gender:'g',browserUsed:'b',locationIP:'ip'])
    people[id].property(VertexProperty.Cardinality.set,'emails','a@example.org')
    people[id].property(VertexProperty.Cardinality.set,'languages','zh')
    edge(people[id],'person_isLocatedIn_place',cities[id == 8 ? 11 : id == 5 ? 22 : 33])
}
[[1L,2L],[3L,1L],[2L,4L],[3L,4L],[4L,5L],[5L,7L],[2L,8L],[3L,9007199254740993L]].each { pair ->
    edge(people[pair[0]],'person_knows_person',people[pair[1]],[creationDate:30L])
}
def base = vertex('tagclass',1000,[name:'Base'])
def child = vertex('tagclass',1001,[name:'Child'])
def other = vertex('tagclass',1002,[name:'Other'])
edge(child,'tagclass_isSubclassOf_tagclass',base)
def tags = [:]
[[10,'A',child],[20,'B',other],[30,'C',child],[40,'Old',other]].each { row ->
    tags[row[0]] = vertex('tag',row[0],[name:row[1]])
    edge(tags[row[0]],'tag_hasType_tagclass',row[2])
}
edge(people[1L],'person_hasInterest_tag',tags[10])
def orgs = [:]
[[700,'Uni',cities[33]],[701,'ZCompany',x],[702,'ACompany',x]].each { row ->
    orgs[row[0]]=vertex('organisation',row[0],[name:row[1],type:row[0]==700?'university':'company'])
    edge(orgs[row[0]],'organisation_isLocatedIn_place',row[2])
}
edge(people[2L],'person_studyAt_organisation',orgs[700],[classYear:2005L])
[[2L,701,2000L],[2L,702,2000L],[4L,701,1999L],[3L,701,2001L]].each { row ->
    edge(people[row[0]],'person_workAt_organisation',orgs[row[1]],[workFrom:row[2]])
}
def forums = [:]
[[500,'F'],[499,'Zero499'],[501,'Zero501']].each { row ->
    forums[row[0]]=vertex('forum',row[0],[title:row[1],creationDate:1L])
    edge(forums[row[0]],'forum_hasModerator_person',people[1L])
}
[[500,2L,101L],[500,3L,100L],[500,4L,200L],[499,2L,102L],[501,9007199254740993L,101L]].each { row ->
    edge(forums[row[0]],'forum_hasMember_person',people[row[1]],[joinDate:row[2]])
}
def posts = [:]
[[1000,2L,100L,x,[10,20]],[1001,2L,200L,y,[10,30]],[1002,3L,50L,x,[40]],
 [1003,3L,150L,y,[40,30]],[1004,4L,175L,x,[10,20]],[1005,8L,180L,y,[10]],
 [1006,1L,120L,z,[10]],[1007,4L,190L,z,[20]],[1008,9007199254740993L,195L,z,[10]],
 [1009,3L,170L,x,[20]]].each { row ->
    def post = vertex('post',row[0],[content:'Post'+row[0],creationDate:row[2],length:8L])
    posts[row[0]]=post
    edge(post,'post_hasCreator_person',people[row[1]])
    edge(post,'post_isLocatedIn_place',row[3])
    edge(forums[500],'forum_containerOf_post',post)
    row[4].each { tag -> edge(post,'post_hasTag_tag',tags[tag]) }
}
def comments = [:]
[[3000,1L,130L,posts[1000]],[3001,2L,210L,posts[1006]],[3002,3L,220L,posts[1006]]].each { row ->
    comments[row[0]] = vertex('comment',row[0],[content:'Comment'+row[0],creationDate:row[2],length:11L])
    edge(comments[row[0]],'comment_hasCreator_person',people[row[1]])
    edge(comments[row[0]],'comment_replyOf_post',row[3])
    edge(comments[row[0]],'comment_isLocatedIn_place',z)
}
[[3003,2L,230L,comments[3000]],[3004,2L,240L,posts[1004]],
 [3005,2L,245L,posts[1003]],[3006,3L,250L,posts[1000]]].each { row ->
    comments[row[0]] = vertex('comment',row[0],[content:'Comment'+row[0],creationDate:row[2],length:11L])
    edge(comments[row[0]],'comment_hasCreator_person',people[row[1]])
    edge(comments[row[0]],'comment_replyOf_'+row[3].label(),row[3])
    edge(comments[row[0]],'comment_isLocatedIn_place',z)
}
[[2L,posts[1006],600120L],[2L,comments[3000],600120L],[3L,posts[1006],600120L],[6L,posts[1006],700120L]].each { row ->
    edge(people[row[0]],'person_likes_'+row[1].label(),row[1],[creationDate:row[2]])
}
graph.tx().commit()
def snapshot = [vertices: g.V().toList().collect { v ->
    Map props = [:]
    v.properties().each { property ->
        if (property.key() in ['emails', 'languages']) {
            if (!props.containsKey(property.key())) props[property.key()] = []
            props[property.key()].add(property.value())
        } else props[property.key()] = property.value()
    }
    [id: v.id(), label: v.label(), properties: props]
}, edges: g.E().toList().collect { edgeValue ->
    Map props = [:]
    edgeValue.properties().each { property -> props[property.key()] = property.value() }
    [outV: edgeValue.outVertex().id(), inV: edgeValue.inVertex().id(), label: edgeValue.label(), properties: props]
}]
graph.tx().commit()
check('IC1 shortest distance, ties, exact long IDs, collections and organizations') {
    def rows=run(1,[personId:1L,firstName:'Same',limit:20])
    assert rows*.friendId == [3L,2L,4L,8L,9007199254740993L]
    assert rows*.distanceFromPerson == [1,1,2,2,2]
    assert rows[1].friendUniversities == [[organizationName:'Uni',year:2005,placeName:'City33']]
    assert rows[1].friendCompanies*.organizationName.toSet() == ['ZCompany','ACompany'] as Set
    assert rows[1].friendEmails == ['a@example.org']
    assert run(1,[personId:1L,firstName:'Same',limit:1]) == rows.take(1)
}
check('IC2 inclusive maxDate and IC9 exclusive maxDate, no self/duplicate foaf') {
    assert run(2,[personId:1L,maxDate:200L,limit:20])*.messageId == [1001L,1009L,1003L,1000L,1002L]
    assert run(9,[personId:1L,maxDate:200L,limit:20])*.messageId == [1008L,1007L,1005L,1004L,1009L,1003L,1000L,1002L]
}
check('IC3 residence exclusion, both-country counts and date interval') {
    def rows=run(3,[personId:1L,countryXName:'X',countryYName:'Y',startDate:100L,durationDays:1,limit:20])
    assert rows*.personId == [2L,3L]
    assert rows*.xCount == [1L,1L] && rows*.yCount == [1L,1L] && rows*.count == [2L,2L]
}
check('IC4 excludes historically used topics and preserves tied alphabetical order') {
    assert run(4,[personId:1L,startDate:100L,durationDays:1,limit:10]) ==
           [[tagName:'A',postCount:2],[tagName:'B',postCount:2],[tagName:'C',postCount:2]]
}
check('IC5 strict membership threshold, all qualifying forums including zero-post ties') {
    assert run(5,[personId:1L,minDate:100L,limit:20]) ==
           [[forumTitle:'F',postCount:4],[forumTitle:'Zero499',postCount:0],[forumTitle:'Zero501',postCount:0]]
    // Big-ID person also belongs to F via neither membership; only 2 and 4 qualify in F: four posts.
}
check('IC6 co-occurrence excludes known tag and counts distinct foaf once') {
    assert run(6,[personId:1L,tagName:'A',limit:10]) == [[tagName:'B',postCount:2],[tagName:'C',postCount:1]]
}
check('IC7 latest per liker, message-ID tie break, latency and new-friend flag') {
    def rows=run(7,[personId:1L,limit:20])
    assert rows*.personId == [6L,2L,3L]
    assert rows*.messageId == [1006L,1006L,1006L]
    assert rows*.minutesLatency == [11,10,10]
    assert rows*.isNew == [true,false,false]
}
check('IC8 direct replies to authored posts AND comments') {
    assert run(8,[personId:1L,limit:20])*.commentId == [3003L,3002L,3001L]
}
check('IC10 exact distance, May and December month wrap, no-common-tag penalty') {
    assert run(10,[personId:1L,month:5,limit:10])*.personId == [4L]
    assert run(10,[personId:1L,month:5,limit:10])[0].commonInterestScore == 0
    assert run(10,[personId:1L,month:12,limit:10])*.personId == [8L]
}
check('IC11 strict work year and descending organization tie break') {
    def rows=run(11,[personId:1L,countryName:'X',workFromYear:2001,limit:10])
    assert rows*.personId == [4L,2L,2L]
    assert rows*.organizationName == ['ZCompany','ZCompany','ACompany']
}
check('IC12 class ancestry, distinct comments and tags, direct reply to post only') {
    def rows=run(12,[personId:1L,tagClassName:'Base',limit:20])
    assert rows*.personId == [2L,3L]
    assert rows*.replyCount == [3,2]
    assert rows[0].tagNames.toSet() == ['A','C'] as Set
}
check('IC13 unreachable and identical endpoints; IC14 all paths and reciprocal weights') {
    assert run(13,[person1Id:1L,person2Id:4L]) == [[shortestPathLength:2]]
    assert run(13,[person1Id:1L,person2Id:6L]) == [[shortestPathLength:-1]]
    assert run(13,[person1Id:1L,person2Id:1L]) == [[shortestPathLength:0]]
    assert run(14,[person1Id:1L,person2Id:4L]) ==
           [[personIdsInPath:[1L,2L,4L],pathWeight:3.5d],[personIdsInPath:[1L,3L,4L],pathWeight:1.0d]]
    assert run(14,[person1Id:1L,person2Id:6L]) == []
    assert run(14,[person1Id:1L,person2Id:1L]) == [[personIdsInPath:[1L],pathWeight:0.0d]]
}
check('IS1 through IS7 shared mapping and projection') {
    assert run(101,[personId:1L])[0].cityId == 33L
    assert run(102,[personId:1L,limit:10])*.messageId == [3000L,1006L]
    assert run(102,[personId:1L,limit:10])[0].originalPostAuthorId == 2L
    assert run(103,[personId:1L])*.personId == [2L,3L]
    assert run(104,[messageId:3003L]) == [[messageContent:'Comment3003',messageCreationDate:230L]]
    assert run(105,[messageId:3003L]) == [[personId:2L,firstName:'Same',lastName:'B']]
    assert run(106,[messageId:3003L])[0].forumId == 500L
    assert run(107,[messageId:1006L])*.commentId == [3002L,3001L]
    assert run(107,[messageId:1006L])*.isReplyAuthorKnowsOriginalMessageAuthor == [true,true]
}
graph.tx().commit()
long newPerson = 9007199254740999L
def personParams = [personId:newPerson,personFirstName:'新',personLastName:'人',gender:'g',birthday:1L,
    creationDate:2L,locationIP:'ip',browserUsed:'b',cityId:33L,languages:['zh','en'],emails:['a','b'],
    tagIds:[10L],studyAt:[[organizationId:700L,year:2010]],workAt:[[organizationId:701L,year:2011]]]
check('IU1 person, sets, organizations and initial/update edge-key separation') {
    assert run(1001,personParams).committed
    assert run(101,[personId:newPerson])[0].firstName == '新'
    def v=g.V('person:'+newPerson).next()
    assert g.V(v.id()).values('emails').toSet() == ['a','b'] as Set
    assert g.V(v.id()).outE().values('edgeKey').toList().every { it < 0L }
}
check('IU4/IU5 forum and membership, including empty tag list') {
    assert run(1004,[forumId:900L,forumTitle:'New',creationDate:3L,moderatorPersonId:newPerson,tagIds:[]]).committed
    assert run(1005,[forumId:900L,personId:newPerson,joinDate:4L]).committed
}
check('IU6 image post, blank properties and IU7 post/comment parent variants') {
    assert run(1006,[postId:9990L,imageFile:'image.png',creationDate:5L,locationIP:'ip',browserUsed:'b',
                    language:'',content:'',length:0,authorPersonId:newPerson,forumId:900L,countryId:300L,tagIds:[10L]]).committed
    assert run(104,[messageId:9990L]) == [[messageContent:'image.png',messageCreationDate:5L]]
    assert run(1007,[commentId:9991L,creationDate:6L,locationIP:'ip',browserUsed:'b',content:'reply',length:5,
                    authorPersonId:2L,countryId:100L,replyToPostId:9990L,replyToCommentId:-1L,tagIds:[10L]]).committed
    assert run(1007,[commentId:9992L,creationDate:7L,locationIP:'ip',browserUsed:'b',content:'nested',length:6,
                    authorPersonId:newPerson,countryId:200L,replyToPostId:-1L,replyToCommentId:9991L,tagIds:[]]).committed
    assert run(106,[messageId:9992L])[0].forumId == 900L
}
check('IU2/IU3 likes, IU8 friendship and subsequent observable reads') {
    assert run(1002,[personId:3L,postId:9990L,creationDate:5000L]).committed
    assert run(1003,[personId:3L,commentId:9991L,creationDate:6000L]).committed
    assert run(1008,[person1Id:newPerson,person2Id:2L,creationDate:7000L]).committed
    assert run(107,[messageId:9990L])[0].isReplyAuthorKnowsOriginalMessageAuthor
    assert run(7,[personId:newPerson,limit:20])[0].personId == 3L
}
graph.tx().commit()
check('Updates roll back partial mutations and reject duplicates/mistyped endpoints') {
    long vertices=g.V().count().next(), edges=g.E().count().next()
    graph.tx().commit()
    fail { run(1001,personParams + [personId:77L,tagIds:[999999L]]) }
    assert !g.V('person:77').hasNext()
    assert g.V().count().next() == vertices && g.E().count().next() == edges
    graph.tx().commit()
    fail { run(1001,personParams) }
    fail { run(1008,[person1Id:2L,person2Id:newPerson,creationDate:7001L]) }
    fail { run(1003,[personId:3L,commentId:9990L,creationDate:6000L]) }
    fail { run(1007,[commentId:77L,replyToPostId:9990L,replyToCommentId:9991L]) }
    assert g.V().count().next() == vertices && g.E().count().next() == edges
}
graph.tx().rollback()
graph.close()
captureVectors = false
check('Random small graphs: bidirectional path results equal independent single-frontier BFS oracle') {
    def random=new Random(3346L)
    for (int sample=0; sample<25; sample++) {
        graph=TinkerTransactionGraph.open(); g=graph.traversal()
        def vs=(0..<9).collect { i -> graph.addVertex(T.id,'person:'+i,T.label,'person','ldbcId',(long)i) }
        List<Set<Integer>> adj=(0..<9).collect { new TreeSet<Integer>() }
        for (int i=0;i<9;i++) for (int j=i+1;j<9;j++) if (random.nextDouble()<0.27) {
            vs[i].addEdge('person_knows_person',vs[j]); adj[i].add(j); adj[j].add(i)
        }
        graph.tx().commit()
        for (int pair=0;pair<5;pair++) {
            int from=random.nextInt(9), to=random.nextInt(9)
            Map<Integer,Integer> distances=[(from):0]
            List<List<Integer>> paths=[[from]], completed=[]
            while (!paths.isEmpty() && completed.isEmpty()) {
                List<List<Integer>> next=[]
                paths.each { path ->
                    int last=path[-1]
                    if (last==to) completed.add(path)
                    else adj[last].each { neighbor ->
                        int distance=path.size()
                        if (!distances.containsKey(neighbor) || distances[neighbor]==distance) {
                            distances[neighbor]=distance; next.add(path+[neighbor])
                        }
                    }
                }
                paths=next
            }
            int distance=completed.isEmpty() ? -1 : completed[0].size()-1
            assert run(13,[person1Id:(long)from,person2Id:(long)to]) == [[shortestPathLength:distance]]
            def actual=run(14,[person1Id:(long)from,person2Id:(long)to])
            assert actual*.personIdsInPath.collect { it.collect { v -> (int)v } }.toSet() == completed.toSet()
            assert actual.size()==completed.size()
            assert actual.every { it.pathWeight==0.0d }
        }
        graph.tx().rollback(); graph.close()
    }
}
assert covered == ((1..14).toList() + (101..107).toList() + (1001..1008).toList()) as Set
if (args.length > 2) {
    java.nio.file.Files.writeString(java.nio.file.Path.of(args[2]),
        JsonOutput.toJson([scope: 'unit-transport-vectors-not-independent-performance-oracle',
                           snapshot: snapshot, operations: vectors]),
        java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE)
}
println JsonOutput.toJson([scope:'ldbc-all-operations-unit-semantics',operations:covered.toList(),
    passed:cases.size(),cases:cases,pathOracleComparisons:250,transactionRollbackVerified:true,
    hugegraphExecuted:false,performanceVerdict:'not-evaluated'])
