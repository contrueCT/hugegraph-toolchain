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

// Initial adapter slice for the frozen SNB v1 mapping. Bind IDs as decimal strings, never JS numbers.
export const queries = Object.freeze({
    IS1: {
        operationKey: "personIdSQ1",
        prefix: "person:",
        gremlin:
            "g.V(vertexId).as('p').out('person_isLocatedIn_place').as('city').select('p').project('firstName','lastName','birthday','locationIp','browserUsed','cityId','gender','creationDate').by('firstName').by('lastName').by('birthday').by('locationIP').by('browserUsed').by(__.select('city').values('ldbcId')).by('gender').by('creationDate')",
    },
    IS2: {
        operationKey: "personIdSQ2",
        prefix: "person:",
        gremlin:
            "g.V(vertexId).in('post_hasCreator_person','comment_hasCreator_person').order().by('creationDate',desc).by('ldbcId',asc).limit(queryLimit).as('m').until(__.hasLabel('post')).repeat(__.out('comment_replyOf_post','comment_replyOf_comment').simplePath()).as('post').out('post_hasCreator_person').as('author').project('messageId','messageContent','messageCreationDate','originalPostId','originalPostAuthorId','originalPostAuthorFirstName','originalPostAuthorLastName').by(__.select('m').values('ldbcId')).by(__.select('m').coalesce(__.values('imageFile'),__.values('content'))).by(__.select('m').values('creationDate')).by(__.select('post').values('ldbcId')).by('ldbcId').by('firstName').by('lastName').order().by('messageCreationDate',desc).by('messageId',asc)",
    },
    IS3: {
        operationKey: "personIdSQ3",
        prefix: "person:",
        gremlin:
            "g.V(vertexId).bothE('person_knows_person').as('friendship').otherV().order().by(__.select('friendship').values('creationDate'),desc).by('ldbcId',asc).project('personId','firstName','lastName','friendshipCreationDate').by('ldbcId').by('firstName').by('lastName').by(__.select('friendship').values('creationDate'))",
    },
    IS4: {
        operationKey: "messageIdContent",
        prefix: "message:",
        gremlin:
            "g.V(vertexId).project('messageContent','messageCreationDate').by(__.coalesce(__.values('content'),__.values('imageFile'))).by('creationDate')",
    },
    IS5: {
        operationKey: "messageIdCreator",
        prefix: "message:",
        gremlin:
            "g.V(vertexId).out('post_hasCreator_person','comment_hasCreator_person').project('personId','firstName','lastName').by('ldbcId').by('firstName').by('lastName')",
    },
    IS6: {
        operationKey: "messageForumId",
        prefix: "message:",
        gremlin:
            "g.V(vertexId).until(__.hasLabel('post')).repeat(__.out('comment_replyOf_post','comment_replyOf_comment').simplePath()).in('forum_containerOf_post').as('forum').out('forum_hasModerator_person').project('forumId','forumTitle','moderatorId','moderatorFirstName','moderatorLastName').by(__.select('forum').values('ldbcId')).by(__.select('forum').values('title')).by('ldbcId').by('firstName').by('lastName')",
    },
    IS7: {
        operationKey: "messageRepliesId",
        prefix: "message:",
        gremlin:
            "g.V(vertexId).as('message').out('post_hasCreator_person','comment_hasCreator_person').as('author').select('message').in('comment_replyOf_post','comment_replyOf_comment').as('comment').out('comment_hasCreator_person').as('replyAuthor').order().by(__.select('comment').values('creationDate'),desc).by('ldbcId',asc).project('commentId','commentContent','commentCreationDate','replyAuthorId','replyAuthorFirstName','replyAuthorLastName','isReplyAuthorKnowsOriginalMessageAuthor').by(__.select('comment').values('ldbcId')).by(__.select('comment').values('content')).by(__.select('comment').values('creationDate')).by('ldbcId').by('firstName').by('lastName').by(__.choose(__.select('author').both('person_knows_person').where(eq('replyAuthor')),__.constant(true),__.constant(false)))",
    },
});

export function request(queryId, decimalId, traversalSource, options = {}) {
    const query = queries[queryId];
    if (!query) throw new Error("Unsupported LDBC query; coverage must remain explicit");
    if (
        typeof decimalId !== "string" ||
        !/^(0|[1-9][0-9]*)$/.test(decimalId) ||
        BigInt(decimalId) > 9223372036854775807n
    )
        throw new Error("Provide a canonical signed-64-bit-range decimal ID string");
    if (typeof traversalSource !== "string" || !/^[A-Za-z_][A-Za-z0-9_.-]{0,127}$/.test(traversalSource))
        throw new Error("Provide the configured traversal source alias explicitly");
    if (
        options === null ||
        Array.isArray(options) ||
        typeof options !== "object" ||
        Object.keys(options).some((key) => queryId !== "IS2" || key !== "limit")
    )
        throw new Error("Only IS2 accepts a limit option");
    const limit = options.limit ?? 10;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 2147483647)
        throw new Error("IS2 limit must be a positive signed-32-bit integer");
    return {
        gremlin: query.gremlin,
        bindings: { vertexId: query.prefix + decimalId, ...(queryId === "IS2" ? { queryLimit: limit } : {}) },
        language: "gremlin-groovy",
        aliases: { g: traversalSource },
    };
}
