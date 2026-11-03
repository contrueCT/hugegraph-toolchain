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

package org.apache.hugegraph.benchmark.reference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.Values;

public final class ExactReferenceMatcherCheck {
    private static int checks;

    private static void check(boolean value) {
        checks++;
        if (!value) { throw new AssertionError("Check " + checks + " failed"); }
    }

    private static void fails(Runnable action) {
        try { action.run(); } catch (IllegalArgumentException e) { checks++; return; }
        throw new AssertionError("Expected rejection");
    }

    private static ExactReferenceMatcher.Expected edge(long value) {
        return new ExactReferenceMatcher.Expected(1, 2, Set.of(), Map.of("creationDate", value));
    }

    private static ExactReferenceMatcher.Observed edgeRecord(long id, long value) {
        return new ExactReferenceMatcher.Observed(id, Set.of("KNOWS"), Map.of("creationDate", value),
                                                 1, 2, Set.of("Person"), Set.of("Person"));
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        Files.createDirectory(root);
        var node = ExactReferenceMatcher.Shape.create("nodes", "Person", List.of(
                "id:ID(Person)", "firstName:STRING", "speaks:STRING[]", "birthday:LONG"));
        var edge = ExactReferenceMatcher.Shape.create("relationships", "KNOWS", List.of(
                ":START_ID(Person)", ":END_ID(Person)", "creationDate:LONG"));
        var row = ExactReferenceMatcher.expected(node, ExactReferenceMatcher.parseLine(
                "9007199254740993|\"a|b\"|en;zh|9223372036854775807"));
        check(row.nodeId() == 9007199254740993L);
        check(row.properties().get("birthday").equals(Long.MAX_VALUE));
        check(row.properties().get("firstName").equals("a|b"));
        check(row.properties().get("speaks").equals(List.of("en", "zh")));
        check(ReferenceGraphVerifier.query(node).contains("(n:`Person` {id:row.id})"));
        check(ReferenceGraphVerifier.query(edge).contains("-[r:`KNOWS`]->(b:`Person`"));
        var postShape = ExactReferenceMatcher.Shape.create("nodes", "Post:Message", List.of("id:ID(Post)"));
        check(ReferenceGraphVerifier.query(postShape).contains("(n:`Post` {id:row.id})"));
        check(ReferenceGraphVerifier.line(new ByteArrayInputStream(
                "中\n".getBytes(StandardCharsets.UTF_8)), 4).equals("中"));
        check(ReferenceGraphVerifier.line(new ByteArrayInputStream(new byte[0]), 4) == null);
        try {
            ReferenceGraphVerifier.line(new ByteArrayInputStream(new byte[]{(byte) 0xff, 10}), 4);
            throw new AssertionError("Malformed UTF-8 accepted");
        } catch (IOException e) { checks++; }
        try {
            ReferenceGraphVerifier.line(new ByteArrayInputStream("abcdef\n".getBytes(StandardCharsets.UTF_8)), 4);
            throw new AssertionError("Oversized row accepted");
        } catch (IllegalArgumentException e) { checks++; }
        try {
            ReferenceGraphVerifier.line(new ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)), 4);
            throw new AssertionError("Missing LF accepted");
        } catch (IllegalArgumentException e) { checks++; }
        var absent = ExactReferenceMatcher.expected(node, ExactReferenceMatcher.parseLine("1|||0"));
        check(!absent.properties().containsKey("firstName") && absent.properties().get("speaks").equals(List.of()));
        var quoted = ExactReferenceMatcher.expected(node, ExactReferenceMatcher.parseLine("1|\"\"|en|0"));
        check(quoted.properties().get("firstName").equals(""));
        var emptyArray = ExactReferenceMatcher.expected(node, ExactReferenceMatcher.parseLine("1|a|\"\"|0"));
        check(emptyArray.properties().get("speaks").equals(List.of()));
        check(ExactReferenceMatcher.parseLine("\"a\"\"b\"|").get(0).value().equals("a\"b"));
        fails(() -> ExactReferenceMatcher.parseLine("\"unterminated"));
        fails(() -> ExactReferenceMatcher.parseLine("a\"b"));
        fails(() -> ExactReferenceMatcher.parseLine("\"a\"b"));
        fails(() -> ExactReferenceMatcher.parseLine("a\nb"));
        fails(() -> ExactReferenceMatcher.integer("9223372036854775808", true));
        fails(() -> ExactReferenceMatcher.integer("01", true));
        fails(() -> ExactReferenceMatcher.integer("-1", true));
        fails(() -> ExactReferenceMatcher.Shape.create("nodes", "Person", List.of("id:ID(Post)")));
        fails(() -> ExactReferenceMatcher.Shape.create("relationships", "KNOWS", List.of(
                ":START_ID(Person)", ":START_ID(Person)", ":END_ID(Person)")));
        var place = ExactReferenceMatcher.Shape.create("nodes", "Place", List.of("id:ID(Place)", ":LABEL"));
        check(ExactReferenceMatcher.expected(place, ExactReferenceMatcher.parseLine("1|City"))
                .labels().equals(Set.of("Place", "City")));
        fails(() -> ExactReferenceMatcher.expected(place, ExactReferenceMatcher.parseLine("1|Company")));
        var smallInt = ExactReferenceMatcher.Shape.create("nodes", "Post", List.of("id:ID(Post)", "length:INT"));
        fails(() -> ExactReferenceMatcher.expected(smallInt, ExactReferenceMatcher.parseLine("1|2147483648")));
        check(ExactReferenceMatcher.property(Values.value(9007199254740993L)).equals(9007199254740993L));
        check(ExactReferenceMatcher.property(Values.value(List.of("x", "y"))).equals(List.of("x", "y")));
        check(ExactReferenceMatcher.properties(Values.value(Map.of("x", 9L))).equals(Map.of("x", 9L)));
        fails(() -> ExactReferenceMatcher.property(Values.value(1.0)));
        fails(() -> ExactReferenceMatcher.property(Values.value(true)));
        fails(() -> ExactReferenceMatcher.property(Values.value(List.of(1L))));
        try (var bitmap = new ExactReferenceMatcher.Bitmap(root.resolve("bounds.bits"), 15)) {
            check(bitmap.claim(0)); check(bitmap.claim(15)); check(!bitmap.claim(15));
            check(bitmap.contains(0) && bitmap.contains(15)); check(bitmap.count() == 2);
            check(bitmap.verifyCardinality() == 2);
            fails(() -> bitmap.claim(16)); fails(() -> bitmap.claim(-1));
        }
        check(Files.size(root.resolve("bounds.bits")) == 2);
        try (var bitmap = new ExactReferenceMatcher.Bitmap(root.resolve("duplicates.bits"), 15)) {
            var batch = new ExactReferenceMatcher.Batch(edge, List.of(edge(10), edge(10)), bitmap, 4);
            batch.accept(0, edgeRecord(1, 10)); batch.accept(1, edgeRecord(1, 10));
            fails(batch::finish);
            batch.accept(1, edgeRecord(2, 10)); check(Arrays.equals(batch.finish(), new long[]{1, 2}));
            var next = new ExactReferenceMatcher.Batch(edge, List.of(edge(10)), bitmap, 4);
            next.accept(0, edgeRecord(1, 10)); fails(next::finish);
            var typed = new ExactReferenceMatcher.Batch(edge, List.of(edge(10)), bitmap, 4);
            typed.accept(0, new ExactReferenceMatcher.Observed(3, Set.of("KNOWS"), Map.of("creationDate", 10L),
                        1, 2, Set.of("Post"), Set.of("Person")));
            fails(typed::finish);
            var reversed = new ExactReferenceMatcher.Batch(edge, List.of(edge(10)), bitmap, 4);
            reversed.accept(0, new ExactReferenceMatcher.Observed(3, Set.of("KNOWS"), Map.of("creationDate", 10L),
                        2, 1, Set.of("Person"), Set.of("Person")));
            fails(reversed::finish);
            var extra = new ExactReferenceMatcher.Batch(edge, List.of(edge(10)), bitmap, 4);
            extra.accept(0, new ExactReferenceMatcher.Observed(3, Set.of("KNOWS"),
                        Map.of("creationDate", 10L, "unexpected", "x"), 1, 2, Set.of("Person"), Set.of("Person")));
            fails(extra::finish);
            var cap = new ExactReferenceMatcher.Batch(edge, List.of(edge(10)), bitmap, 1);
            cap.accept(0, null); fails(() -> cap.accept(0, null)); fails(() -> cap.accept(1, null));
        }
        var nativeValues = new ObjectMapper().readTree(Path.of(args[1]).toFile());
        int nativeCases = 0;
        for (var record : nativeValues.path("observations")) {
            var nativeShape = ExactReferenceMatcher.Shape.create("nodes", "Person",
                    List.of("id:ID(Person)", "value:" + record.path("type").asText()));
            var expected = ExactReferenceMatcher.expected(nativeShape, List.of(
                    new ExactReferenceMatcher.Field("1", false),
                    new ExactReferenceMatcher.Field(record.path("input").asText(), record.path("quoted").asBoolean())));
            check(expected.properties().containsKey("value") == record.path("present").asBoolean());
            if (record.path("present").asBoolean()) {
                check(new ObjectMapper().valueToTree(expected.properties().get("value")).equals(record.path("value")));
            }
            nativeCases++;
        }
        check(nativeCases == 24 && !nativeValues.path("databaseOpened").asBoolean());
        // Exhaustive small multiset oracle, independent of the streaming greedy matching algorithm.
        int cases = 0;
        for (int e = 0; e < 16; e++) {
            for (int a = 0; a < 16; a++) {
                long[] want = {e & 1, (e >> 1) & 1, (e >> 2) & 1, (e >> 3) & 1};
                long[] got = {a & 1, (a >> 1) & 1, (a >> 2) & 1, (a >> 3) & 1};
                long[] wantSorted = want.clone(); Arrays.sort(wantSorted);
                long[] gotSorted = got.clone(); Arrays.sort(gotSorted);
                Path file = root.resolve("oracle-" + e + "-" + a + ".bits");
                try (var bitmap = new ExactReferenceMatcher.Bitmap(file, 7)) {
                    var batch = new ExactReferenceMatcher.Batch(edge,
                            Arrays.stream(want).mapToObj(ExactReferenceMatcherCheck::edge).toList(), bitmap, 4);
                    // Interleave candidate arrival across source rows and reverse database ID order.
                    for (int j = 3; j >= 0; j--) {
                        for (int i = 3; i >= 0; i--) { batch.accept(i, edgeRecord(j, got[j])); }
                    }
                    boolean accepted;
                    try { batch.finish(); accepted = true; } catch (IllegalArgumentException ex) { accepted = false; }
                    check(accepted == Arrays.equals(wantSorted, gotSorted));
                    if (accepted) { check(bitmap.count() == 4 && bitmap.verifyCardinality() == 4); }
                }
                Files.delete(file); cases++;
            }
        }
        try (var bitmap = new ExactReferenceMatcher.Bitmap(root.resolve("node.bits"), 15)) {
            var batch = new ExactReferenceMatcher.Batch(node, List.of(row), bitmap, 2);
            var wrong = new TreeMap<>(row.properties()); wrong.put("id", 9007199254740992L);
            batch.accept(0, new ExactReferenceMatcher.Observed(1, row.labels(), wrong, -1, -1, Set.of(), Set.of()));
            fails(batch::finish);
            batch.accept(0, new ExactReferenceMatcher.Observed(2, row.labels(), row.properties(),
                                                              -1, -1, Set.of(), Set.of()));
            check(batch.finish()[0] == 2);
        }
        System.out.println("{\"checks\":" + checks + ",\"exhaustiveMultisetCases\":" + cases +
                           ",\"nativeCsvCases\":" + nativeCases +
                           ",\"databaseConnections\":0,\"performanceAccepted\":false}");
    }
}
