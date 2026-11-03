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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Pure loopback protocol reproduction. A counted update is only a local integer, never a graph write. */
public final class TransportRetryCheck {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static byte[] header(InputStream stream) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int suffix = 0;
        byte[] end = {13, 10, 13, 10};
        while (bytes.size() < 65536) {
            int value = stream.read();
            if (value < 0) return null;
            bytes.write(value);
            suffix = value == end[suffix] ? suffix + 1 : value == 13 ? 1 : 0;
            if (suffix == 4) return bytes.toByteArray();
        }
        throw new IllegalStateException("Header limit");
    }

    private static void require(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    public static void main(String[] args) throws Exception {
        AtomicInteger ordinal = new AtomicInteger();
        AtomicInteger updates = new AtomicInteger();
        AtomicBoolean closing = new AtomicBoolean();
        List<Map<String, Object>> observed = new CopyOnWriteArrayList<>();
        List<Socket> sockets = new CopyOnWriteArrayList<>();
        byte[] response = "{\"status\":{\"code\":200},\"result\":{\"data\":[]}}"
                .getBytes(StandardCharsets.UTF_8);
        ServerSocket server = new ServerSocket(0, 4, java.net.InetAddress.getByName("127.0.0.1"));
        Thread listener = new Thread(() -> {
            while (!closing.get()) {
                try {
                    Socket socket = server.accept();
                    sockets.add(socket);
                    int connection = ordinal.incrementAndGet();
                    Thread handler = new Thread(() -> {
                        try (socket) {
                            socket.setSoTimeout(3000);
                            InputStream input = socket.getInputStream();
                            while (true) {
                                byte[] raw = header(input);
                                if (raw == null) return;
                                String headers = new String(raw, StandardCharsets.ISO_8859_1);
                                int length = -1;
                                for (String line : headers.split("\r\n")) {
                                    if (line.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:")) {
                                        length = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                                    }
                                }
                                if (length < 0 || length > 1048576) throw new IllegalStateException("Body limit");
                                byte[] body = input.readNBytes(length);
                                if (body.length != length) throw new IllegalStateException("Incomplete body");
                                int type = JSON.readTree(body).path("bindings").path("operationType").intValue();
                                observed.add(Map.of("connection", connection, "operationType", type,
                                        "bodySha256", HexFormat.of().formatHex(
                                                MessageDigest.getInstance("SHA-256").digest(body))));
                                if (type == 1008 && updates.incrementAndGet() == 1) {
                                    return; // Count the synthetic update, then lose its response.
                                }
                                socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: " +
                                        response.length + "\r\nContent-Type: application/json\r\n\r\n")
                                        .getBytes(StandardCharsets.ISO_8859_1));
                                socket.getOutputStream().write(response);
                                socket.getOutputStream().flush();
                            }
                        } catch (Exception ignored) {
                            // EOF/timeout is expected when the diagnostic client closes.
                        }
                    }, "protocol-handler");
                    handler.setDaemon(true);
                    handler.start();
                } catch (Exception ignored) {
                    return;
                }
            }
        }, "protocol-listener");
        listener.setDaemon(true);
        listener.start();
        String failure = null;
        boolean returned = false;
        boolean rejected = false;
        try (HttpTransport client = new HttpTransport(Map.of(
                "hgbench.endpoint", "http://127.0.0.1:" + server.getLocalPort() + "/gremlin",
                "hgbench.alias", "__g_DEFAULT_protocol", "hgbench.timeoutMs", "1500",
                "hgbench.maxResponseBytes", "4096", "hgbench.httpThreads", "2"))) {
            client.execute("protocol-only", 1, Map.of());
            Thread.sleep(50); // Let the completed first response return its connection to the pool.
            try {
                client.execute("protocol-only", 1008, Map.of());
                returned = true;
            } catch (Exception error) {
                failure = error.getClass().getSimpleName();
            }
        } catch (IllegalArgumentException error) {
            require(error.getMessage().contains("-Djdk.httpclient."), "Unexpected constructor failure");
            rejected = true;
            failure = error.getClass().getSimpleName();
        } finally {
            closing.set(true);
            server.close();
            for (Socket socket : sockets) socket.close();
            listener.join(2000);
        }
        boolean expectedRejection = args.length == 1 && args[0].equals("reject");
        require(rejected == expectedRejection, "Retry policy admission differs from expected");
        if (expectedRejection) {
            require(observed.isEmpty() && updates.get() == 0, "Rejected policy contacted the server");
        } else {
            require(observed.size() == 2 && updates.get() == 1 && !returned,
                    "Unknown response must fail after exactly one synthetic update delivery");
            require(observed.get(0).get("connection").equals(observed.get(1).get("connection")),
                    "The unknown update did not exercise a reused connection");
        }
        System.out.write(JSON.writeValueAsBytes(Map.of(
                "scope", "Loopback counted requests only; no graph or real update",
                "enableAllMethodRetry", System.getProperty("jdk.httpclient.enableAllMethodRetry", "unset"),
                "disableRetryConnect", System.getProperty("jdk.httpclient.disableRetryConnect", "unset"),
                "syntheticUpdateDeliveries", updates.get(), "updateReturned", returned, "constructorRejected", rejected,
                "failure", failure == null ? "none" : failure, "requests", new ArrayList<>(observed))));
        System.out.write('\n');
    }
}
