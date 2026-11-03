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

package org.apache.hugegraph.benchmark.ldbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.LoggingService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Adapter only: workload generation, scheduling, dependencies and measurement remain in the official driver. */
public final class HugeGraphDb extends Db {
    private Connection connection;

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        try {
            connection = new Connection(properties);
            for (int type : connection.contract.operationTypes()) {
                Class operation = Class.forName("org.ldbcouncil.snb.driver.workloads.interactive." +
                                                connection.contract.className(type));
                registerOperationHandler(operation, Handler.class);
            }
        } catch (Exception error) {
            if (connection != null) {
                connection.close();
            }
            throw new DbException("Cannot initialize HG-Bench adapter", error);
        }
    }

    @Override
    protected void onClose() {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected DbConnectionState getConnectionState() {
        return connection;
    }

    public static final class Connection extends DbConnectionState {
        final ResultContract contract;
        final HttpTransport transport;
        final ObjectMapper mapper = new ObjectMapper();
        final String reads;
        final String updates;
        final AtomicReference<Exception> failure = new AtomicReference<>();

        public Connection(Map<String, String> properties) throws IOException {
            contract = new ResultContract();
            reads = resource("/snb-read.groovy");
            updates = resource("/snb-update.groovy");
            transport = new HttpTransport(properties);
        }

        @Override
        public void close() {
            transport.close();
        }

        private static String resource(String path) throws IOException {
            try (InputStream input = Connection.class.getResourceAsStream(path)) {
                if (input == null) {
                    throw new IOException("Missing bundled query script: " + path);
                }
                return new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
    }

    public static final class Handler implements OperationHandler<Operation<Object>, Connection> {
        @Override
        public void executeOperation(Operation<Object> operation, Connection state, ResultReporter reporter)
                throws DbException {
            if (state.failure.get() != null) {
                throw new DbException("Attempt already failed; restore state before a new attempt",
                                      state.failure.get());
            }
            try {
                Map<String, Object> parameters = operation.parameterMap();
                String script = operation.type() >= 1000 ? state.updates : state.reads;
                JsonNode data = state.transport.execute(script, operation.type(), parameters);
                JsonNode result = state.contract.validate(operation.type(), data, parameters);
                String serialized = result == null ? "\"-1\"" : state.mapper.writeValueAsString(result);
                Object decoded = operation.deserializeResult(serialized);
                reporter.report(0, decoded, operation);
            } catch (Exception error) {
                state.failure.compareAndSet(null, error);
                // In-flight updates may still commit after timeout; never retry or continue this attempt as valid.
                state.transport.close();
                throw new DbException("Operation failed; update outcome may be unknown; restore before retry", error);
            }
        }
    }
}
