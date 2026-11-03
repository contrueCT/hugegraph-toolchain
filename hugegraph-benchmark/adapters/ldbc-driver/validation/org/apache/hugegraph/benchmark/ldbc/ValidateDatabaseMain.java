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
package org.apache.hugegraph.benchmark.ldbc;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ldbcouncil.snb.driver.Client;
import org.ldbcouncil.snb.driver.client.ClientMode;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.ControlService;
import org.ldbcouncil.snb.driver.control.DriverConfiguration;
import org.ldbcouncil.snb.driver.control.LocalControlService;
import org.ldbcouncil.snb.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.snb.driver.csv.simple.SimpleCsvFileReader;
import org.ldbcouncil.snb.driver.temporal.SystemTimeSource;
import org.ldbcouncil.snb.driver.validation.DbValidationResult;
import org.ldbcouncil.snb.driver.validation.ValidationParamsFromCsvRows;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;

/** Enforce the official ordered validator's returned status without changing its implementation. */
public final class ValidateDatabaseMain {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final class CheckFailure extends IllegalArgumentException {
        CheckFailure(String message) {
            super(message);
        }
    }

    private static void require(boolean value, String message) {
        if (!value) {
            throw new CheckFailure(message);
        }
    }

    private static String sha(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream stream = new DigestInputStream(Files.newInputStream(path), digest)) {
            stream.transferTo(java.io.OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static Map<Integer, Long> coverage(Path path) throws Exception {
        Map<Integer, Long> counts = new TreeMap<>();
        var expected = LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping();
        try (var reader = new SimpleCsvFileReader(path.toFile(),
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING)) {
            var rows = new ValidationParamsFromCsvRows(reader, null);
            while (rows.hasNext()) {
                var operation = rows.next().operation();
                require(expected.get(operation.type()) == operation.getClass(), "Unknown validation operation");
                counts.merge(operation.type(), 1L, Math::addExact);
            }
        }
        return counts;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.print("ValidateDatabaseMain --out NEW_DIRECTORY -- OFFICIAL_DRIVER_ARGUMENTS\n" +
                    "Requires mode=validate_database and all 29 SNB v1 operation types.\n" +
                    "Copies validation CSV into NEW_DIRECTORY; official failure files stay beside that copy.\n" +
                    "Uses unchanged official Client/ValidateDatabaseMode/DbValidator, checks isSuccessful().\n" +
                    "HTTP JVM options: -Djdk.httpclient.disableRetryConnect=true " +
                    "-Djdk.httpclient.enableAllMethodRetry=false.\n" +
                    "Exit 0 = ordered answers matched; exit 4 = incomplete/failed, no automatic retry.\n" +
                    "Updates in the validation file are executed. Restore the target before a new attempt.\n" +
                    "External CPU/memory/disk/log/deadline supervision and reference provenance are required.\n" +
                    "This receipt does not establish SF100 data identity, independent provenance or performance.\n");
            return;
        }
        require(args.length >= 4 && args[0].equals("--out") && args[2].equals("--"),
                "Expected --out NEW_DIRECTORY -- OFFICIAL_DRIVER_ARGUMENTS; use --help");
        Path out = Path.of(args[1]).toAbsolutePath();
        Files.createDirectory(out);
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("schemaVersion", 1);
        receipt.put("kind", "official-ldbc-ordered-validation");
        receipt.put("startedAt", Instant.now().toString());
        receipt.put("officialModeInvoked", false);
        receipt.put("validationSuccessful", false);
        receipt.put("performanceAccepted", false);
        receipt.put("referenceProvenanceVerified", false);
        ControlService control = null;
        int exit = 4;
        try {
            DriverConfiguration config = ConsoleAndFileDriverConfiguration.fromArgs(
                    Arrays.copyOfRange(args, 3, args.length));
            require(config.mode().equals("validate_database") && !config.shouldPrintHelpString(),
                    "Use explicit mode=validate_database; generation and measurement use the official Client");
            require(LdbcSnbInteractiveWorkload.class.getName().equals(config.workloadClassName()),
                    "Requires the pinned SNB Interactive v1 workload");
            require(config.databaseValidationFilePath() != null, "Missing validate_database CSV path");
            Path source = Path.of(config.databaseValidationFilePath()).toAbsolutePath();
            require(Files.isRegularFile(source) && !Files.isSymbolicLink(source), "Expected regular validation CSV");
            String inputSha = sha(source);
            Path copy = out.resolve("validation.csv");
            Files.copy(source, copy);
            require(sha(copy).equals(inputSha) && sha(source).equals(inputSha), "Validation input changed during copy");
            receipt.put("validationSha256", inputSha);
            Map<Integer, Long> counts = coverage(copy);
            receipt.put("operationCounts", counts);
            receipt.put("records", counts.values().stream().mapToLong(Long::longValue).sum());
            var expectedTypes = LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping().keySet();
            require(counts.keySet().equals(expectedTypes) && counts.size() == 29,
                    "Validation must cover all 29 SNB Interactive v1 operation types; " +
                    "empty/subset files are incomplete");
            config = config.applyArgs(Map.of(ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                    copy.toString(), ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                    out.resolve("official-results").toString()));
            receipt.put("databaseClass", config.dbClassName());
            System.setProperty("jdk.httpclient.disableRetryConnect", "true");
            System.setProperty("jdk.httpclient.enableAllMethodRetry", "false");
            var clock = new SystemTimeSource();
            control = new LocalControlService(clock.nowAsMilli() + 5000, config,
                    new Log4jLoggingServiceFactory(false), clock);
            ClientMode<?> mode = new Client().getClientModeFor(control);
            mode.init();
            receipt.put("officialModeInvoked", true);
            Object returned = mode.startExecutionAndAwaitCompletion();
            require(returned instanceof DbValidationResult, "Official mode returned no validation result");
            DbValidationResult result = (DbValidationResult) returned;
            receipt.put("officialResultMessage", result.resultMessage());
            require(sha(copy).equals(inputSha), "Validation copy changed during execution");
            require(result.isSuccessful(), "Official validator reported failed operations");
            receipt.put("validationSuccessful", true);
            exit = 0;
        } catch (Exception failure) {
            receipt.put("errorClass", failure.getClass().getSimpleName());
            if (failure instanceof CheckFailure) {
                receipt.put("error", failure.getMessage());
            }
            // Do not duplicate arbitrary configuration/credential values into the receipt.
            System.err.println("Ordered validation failed (" + failure.getClass().getSimpleName() +
                    "); inspect this attempt's official logs and receipt.json. Do not replay updates.");
        } finally {
            if (control != null) {
                control.shutdown();
            }
            receipt.put("finishedAt", Instant.now().toString());
            receipt.put("state", exit == 0 ? "answers-matched" : "failed-no-replay");
            Files.write(out.resolve("receipt.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(receipt),
                        StandardOpenOption.CREATE_NEW);
        }
        System.exit(exit);
    }
}
