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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.csv.simple.SimpleCsvFileReader;
import org.ldbcouncil.snb.driver.validation.ValidationParam;
import org.ldbcouncil.snb.driver.validation.ValidationParamsFromCsvRows;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery13Result;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;

/** Protocol/fault stub only. No graph, sockets, storage backend or performance oracle. */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ValidationFixtureDb extends Db {
    private Connection connection;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService logging) throws DbException {
        try {
            this.connection = new Connection(properties);
            for (var entry : LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping().entrySet()) {
                if (!this.connection.mode.equals("missing") || entry.getKey() != 14) {
                    registerOperationHandler((Class) entry.getValue(), Handler.class);
                }
            }
        } catch (Exception error) {
            throw new DbException(error);
        }
    }

    @Override
    protected void onClose() throws IOException {
        this.connection.close();
    }

    @Override
    protected DbConnectionState getConnectionState() {
        return this.connection;
    }

    public static final class Connection extends DbConnectionState {
        final String mode;
        final List<ValidationParam> rows = new ArrayList<>();
        final Path calls;
        final Path validation;
        int next;

        Connection(Map<String, String> properties) throws Exception {
            this.mode = properties.get("fixture.mode");
            this.calls = Path.of(properties.get("fixture.calls"));
            this.validation = Path.of(properties.get(ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG));
            Files.writeString(this.calls, "init\n", StandardOpenOption.CREATE_NEW);
            try (var reader = new SimpleCsvFileReader(Path.of(properties.get("fixture.reference")).toFile(),
                    SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING)) {
                var parsed = new ValidationParamsFromCsvRows(reader, null);
                while (parsed.hasNext()) {
                    var row = parsed.next();
                    if (!this.mode.equals("missing") || row.operation().type() != 14) {
                        this.rows.add(row);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            Files.writeString(this.calls, "close\n", StandardOpenOption.APPEND);
            if (this.mode.equals("mutate")) {
                Files.writeString(this.validation, "\n", StandardOpenOption.APPEND);
            }
        }
    }

    public static final class Handler implements OperationHandler<Operation, Connection> {
        @Override
        public void executeOperation(Operation operation, Connection state, ResultReporter reporter)
                throws DbException {
            try {
                ValidationParam row = state.rows.get(state.next++);
                if (!operation.equals(row.operation())) {
                    throw new DbException("Official validator changed record order");
                }
                Files.writeString(state.calls, operation.type() + "\n", StandardOpenOption.APPEND);
                if (state.mode.equals("throw") && operation.type() == 1008) {
                    throw new DbException("Injected operation failure; no graph writes");
                }
                Object result = state.mode.equals("wrong") && operation.type() == 13
                                ? new LdbcQuery13Result(-999) : row.operationResult();
                reporter.report(0, result, operation);
            } catch (IOException error) {
                throw new DbException(error);
            }
        }
    }
}
