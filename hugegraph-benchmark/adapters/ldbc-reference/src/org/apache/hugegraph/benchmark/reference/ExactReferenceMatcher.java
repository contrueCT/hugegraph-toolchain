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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.neo4j.driver.Value;

/** Exact source-to-database bijection. This class never opens a database connection. */
public final class ExactReferenceMatcher {
    private ExactReferenceMatcher() { }

    public record Field(String value, boolean quoted) { }

    public record Shape(String kind, String label, List<String> columns, String sourceLabel,
                        String targetLabel, Set<String> nodeLabels) {
        public static Shape create(String kind, String label, List<String> columns) {
            require(kind.equals("nodes") || kind.equals("relationships"), "Unknown table kind");
            require(!columns.isEmpty() && columns.size() <= 32, "Invalid table column count");
            String source = null;
            String target = null;
            Set<String> nodeLabels = new TreeSet<>();
            if (kind.equals("nodes")) {
                nodeLabels.addAll(Arrays.asList(label.split(":")));
                require(!nodeLabels.isEmpty(), "Node labels are required");
            } else {
                name(label);
            }
            for (String value : nodeLabels) { name(value); }
            Set<String> properties = new HashSet<>();
            int ids = 0;
            int labels = 0;
            for (String column : columns) {
                if (column.startsWith(":START_ID(")) {
                    require(source == null, "Repeated source endpoint");
                    source = group(column, ":START_ID(");
                } else if (column.startsWith(":END_ID(")) {
                    require(target == null, "Repeated target endpoint");
                    target = group(column, ":END_ID(");
                } else if (column.equals(":LABEL")) {
                    labels++;
                } else {
                    int colon = column.indexOf(':');
                    require(colon > 0, "Missing property type");
                    String key = column.substring(0, colon);
                    name(key);
                    require(properties.add(key), "Repeated property name");
                    String type = column.substring(colon + 1);
                    if (type.startsWith("ID(")) {
                        require(key.equals("id"), "Expected public id property");
                        require(nodeLabels.contains(group(type, "ID(")), "ID group must be a node base label");
                        ids++;
                    } else {
                        require(Set.of("STRING", "STRING[]", "LONG", "INT").contains(type),
                                "Unsupported reference property type");
                    }
                }
            }
            require(kind.equals("nodes") ? ids == 1 && source == null && target == null && labels <= 1 :
                    ids == 0 && source != null && target != null && labels == 0,
                    "Header does not match its node/relationship role");
            return new Shape(kind, label, List.copyOf(columns), source, target, Set.copyOf(nodeLabels));
        }
    }

    public record Expected(long sourceId, long targetId, Set<String> labels, Map<String, Object> properties) {
        public long nodeId() { return (Long) this.properties.get("id"); }
    }

    public record Observed(long internalId, Set<String> labels, Map<String, Object> properties,
                           long sourceId, long targetId, Set<String> sourceLabels, Set<String> targetLabels) { }

    public static void require(boolean value, String message) {
        if (!value) { throw new IllegalArgumentException(message); }
    }

    public static void name(String value) {
        require(value.matches("[A-Za-z][A-Za-z0-9_]{0,95}"), "Invalid schema identifier");
    }

    private static String group(String column, String prefix) {
        require(column.startsWith(prefix) && column.endsWith(")"), "Invalid ID group");
        String result = column.substring(prefix.length(), column.length() - 1);
        name(result);
        return result;
    }

    public static long integer(String value, boolean nonnegative) {
        require(value.matches("-?(0|[1-9][0-9]*)"), "Non-canonical integer");
        long result = Long.parseLong(value);
        require(!nonnegative || result >= 0, "Negative public graph ID");
        return result;
    }

    /** Strict one-line pipe CSV, matching the frozen converter's RFC-style doubled quotes. */
    public static List<Field> parseLine(String line) {
        require(line.indexOf('\n') < 0 && line.indexOf('\r') < 0 && line.indexOf('\0') < 0,
                "Multiline/NUL CSV is unsupported");
        List<Field> result = new ArrayList<>();
        int i = 0;
        while (true) {
            StringBuilder value = new StringBuilder();
            boolean quoted = i < line.length() && line.charAt(i) == '"';
            if (quoted) {
                i++;
                boolean closed = false;
                while (i < line.length()) {
                    char c = line.charAt(i++);
                    if (c == '"') {
                        if (i < line.length() && line.charAt(i) == '"') {
                            value.append('"'); i++;
                        } else {
                            closed = true; break;
                        }
                    } else { value.append(c); }
                }
                require(closed && (i == line.length() || line.charAt(i) == '|'), "Unclosed/invalid CSV quote");
            } else {
                while (i < line.length() && line.charAt(i) != '|') {
                    char c = line.charAt(i++);
                    require(c != '"', "Bare quote in unquoted CSV field");
                    value.append(c);
                }
            }
            result.add(new Field(value.toString(), quoted));
            require(result.size() <= 32, "Excessive CSV column count");
            if (i == line.length()) { return result; }
            i++;
        }
    }

    public static Expected expected(Shape shape, List<Field> row) {
        require(row.size() == shape.columns.size(), "CSV width differs from the frozen header");
        Map<String, Object> props = new TreeMap<>();
        Set<String> labels = new TreeSet<>(shape.nodeLabels);
        long sourceId = -1;
        long targetId = -1;
        for (int i = 0; i < row.size(); i++) {
            String column = shape.columns.get(i);
            Field field = row.get(i);
            String value = field.value;
            if (column.startsWith(":START_ID(")) {
                sourceId = integer(value, true);
            } else if (column.startsWith(":END_ID(")) {
                targetId = integer(value, true);
            } else if (column.equals(":LABEL")) {
                require(shape.nodeLabels.contains("Place") ? Set.of("City", "Country", "Continent").contains(value) :
                        shape.nodeLabels.contains("Organisation") && Set.of("Company", "University").contains(value),
                        "Unexpected LDBC subtype label");
                require(labels.add(value), "Repeated subtype label");
            } else {
                int split = column.indexOf(':');
                String key = column.substring(0, split);
                String type = column.substring(split + 1);
                if (type.equals("STRING") || type.equals("STRING[]")) {
                    // Neo4j 4.4: empty STRING[] remains []; unquoted empty STRING is absent.
                    if (type.equals("STRING") && value.isEmpty() && !field.quoted) { continue; }
                    props.put(key, type.equals("STRING") ? value :
                                   value.isEmpty() ? List.of() : List.of(value.split(";", -1)));
                } else {
                    long number = integer(value, type.startsWith("ID("));
                    require(!type.equals("INT") || number == (int) number, "INT property outside signed 32-bit range");
                    props.put(key, number);
                }
            }
        }
        return new Expected(sourceId, targetId, Set.copyOf(labels), Map.copyOf(props));
    }

    /** Reject lossy numeric coercions and unexpected Neo4j property kinds. */
    public static Object property(Value value) {
        String type = value.type().name();
        return switch (type) {
            case "INTEGER" -> value.asLong();
            case "STRING" -> value.asString();
            case "LIST OF ANY?", "LIST" -> {
                List<String> list = new ArrayList<>();
                for (Value item : value.values()) {
                    require(item.type().name().equals("STRING"), "Expected string-array element");
                    list.add(item.asString());
                }
                yield list;
            }
            default -> throw new IllegalArgumentException("Unexpected Neo4j property type: " + type);
        };
    }

    public static Map<String, Object> properties(Value map) {
        require(map.type().name().equals("MAP"), "Expected property map");
        Map<String, Object> result = new TreeMap<>();
        for (String key : map.keys()) { result.put(key, property(map.get(key))); }
        return result;
    }

    public interface Claimed extends Closeable {
        boolean contains(long id);
        boolean claim(long id);
        long count();
    }

    /** Fixed-size mapped bitmap: no growing hash table and no implicit extension past the budget. */
    public static final class Bitmap implements Claimed {
        private final FileChannel channel;
        private final MappedByteBuffer bytes;
        private final long maxId;
        private long count;

        public Bitmap(Path path, long maxId) throws IOException {
            require(maxId >= 0 && maxId <= Integer.MAX_VALUE, "Internal ID bitmap limit outside supported range");
            this.maxId = maxId;
            long size = (maxId >>> 3) + 1;
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                                            StandardOpenOption.READ, StandardOpenOption.WRITE);
            try {
                this.channel.write(ByteBuffer.wrap(new byte[1]), size - 1);
                this.bytes = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            } catch (IOException | RuntimeException e) {
                this.channel.close(); throw e;
            }
        }

        private int offset(long id) {
            require(id >= 0 && id <= this.maxId, "Database internal ID exceeds frozen bitmap limit");
            return (int) (id >>> 3);
        }

        @Override
        public boolean contains(long id) {
            return (this.bytes.get(offset(id)) & (1 << (id & 7))) != 0;
        }

        @Override
        public boolean claim(long id) {
            int offset = offset(id);
            int mask = 1 << (id & 7);
            byte before = this.bytes.get(offset);
            if ((before & mask) != 0) { return false; }
            this.bytes.put(offset, (byte) (before | mask));
            this.count++;
            return true;
        }

        @Override
        public long count() { return this.count; }

        public long verifyCardinality() {
            long total = 0;
            for (int i = 0; i < this.bytes.capacity(); i++) {
                total += Integer.bitCount(Byte.toUnsignedInt(this.bytes.get(i)));
            }
            require(total == this.count, "Bitmap cardinality changed");
            return total;
        }

        @Override
        public void close() throws IOException {
            this.bytes.force();
            this.channel.close();
        }
    }

    public static final class Batch {
        private final Shape shape;
        private final List<Expected> expected;
        private final Claimed claimed;
        private final int maxCandidatesPerRow;
        private final int[] candidates;
        private final long[] selected;

        public Batch(Shape shape, List<Expected> expected, Claimed claimed, int maxCandidatesPerRow) {
            require(!expected.isEmpty() && expected.size() <= 10000, "Invalid expected batch size");
            require(maxCandidatesPerRow > 0 && maxCandidatesPerRow <= 10000, "Invalid candidate limit");
            this.shape = shape;
            this.expected = List.copyOf(expected);
            this.claimed = claimed;
            this.maxCandidatesPerRow = maxCandidatesPerRow;
            this.candidates = new int[expected.size()];
            this.selected = new long[expected.size()];
            Arrays.fill(this.selected, -1);
        }

        public void accept(int ordinal, Observed actual) {
            require(ordinal >= 0 && ordinal < this.expected.size(), "Undeclared result ordinal");
            require(++this.candidates[ordinal] <= this.maxCandidatesPerRow, "Candidate fanout limit exhausted");
            if (actual == null || this.selected[ordinal] >= 0) { return; }
            Expected want = this.expected.get(ordinal);
            boolean exact = want.properties.equals(actual.properties);
            if (this.shape.kind.equals("nodes")) {
                exact &= want.labels.equals(actual.labels);
            } else {
                exact &= want.sourceId == actual.sourceId && want.targetId == actual.targetId &&
                         actual.sourceLabels.contains(this.shape.sourceLabel) &&
                         actual.targetLabels.contains(this.shape.targetLabel) &&
                         actual.labels.equals(Set.of(this.shape.label));
            }
            if (exact && this.claimed.claim(actual.internalId)) { this.selected[ordinal] = actual.internalId; }
        }

        public long[] finish() {
            for (int i = 0; i < this.selected.length; i++) {
                require(this.selected[i] >= 0, "No distinct exact database match for input ordinal " + i);
            }
            return this.selected.clone();
        }
    }
}
