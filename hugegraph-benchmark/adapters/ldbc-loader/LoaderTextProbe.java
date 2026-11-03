/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.hugegraph.loader.source.InputSource;
import org.apache.hugegraph.loader.util.DataTypeUtil;
import org.apache.hugegraph.structure.constant.DataType;

/** Verifies the actual native class or packaged overlay without opening a database. */
public final class LoaderTextProbe {

    public static void main(String[] args) throws Exception {
        boolean preserve = args[0].equals("preserve");
        Path origin = Path.of(DataTypeUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!origin.toRealPath().equals(Path.of(args[2]).toRealPath())) {
            throw new AssertionError("Unexpected DataTypeUtil implementation: " + origin);
        }
        Method parse = DataTypeUtil.class.getDeclaredMethod("parseSingleValue", String.class, Object.class,
                                                          DataType.class, InputSource.class);
        parse.setAccessible(true);
        int checks = 0;
        for (String value : List.of(" About 甲 ", "\t\r\ntext \n", " \t", "", "\u001f甲\u001f", "A\u2000 ")) {
            Object actual = parse.invoke(null, "content", value, DataType.TEXT, null);
            if (!(preserve ? value : value.trim()).equals(actual)) {
                throw new AssertionError("Text whitespace contract differs");
            }
            checks++;
        }
        if (!Long.valueOf(9007199254740993L).equals(
                parse.invoke(null, "longValue", " 9007199254740993 ", DataType.LONG, null))) {
            throw new AssertionError("Long parsing lost precision or whitespace handling");
        }
        checks++;
        if (!Boolean.TRUE.equals(parse.invoke(null, "booleanValue", " true ", DataType.BOOLEAN, null))) {
            throw new AssertionError("Boolean parsing changed");
        }
        checks++;
        String json = String.format("{\"checks\":%d,\"mode\":\"%s\",\"implementation\":\"%s\"}%n",
                                    checks, args[0], origin.toUri());
        Files.writeString(Path.of(args[1]), json, StandardCharsets.UTF_8);
    }
}
