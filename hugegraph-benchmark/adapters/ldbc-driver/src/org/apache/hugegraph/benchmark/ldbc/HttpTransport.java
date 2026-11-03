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
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** One POST per operation, bounded complete-body consumption, and no operation-level retries. */
public final class HttpTransport implements AutoCloseable {
    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY,
            DeserializationFeature.FAIL_ON_TRAILING_TOKENS, DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final HttpClient client;
    private final ExecutorService executor;
    private final URI endpoint;
    private final String alias;
    private final String authorization;
    private final int timeoutMillis;
    private final int maxResponseBytes;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<CompletableFuture<?>> pending = ConcurrentHashMap.newKeySet();

    public HttpTransport(Map<String, String> properties) {
        if (!"true".equals(System.getProperty("jdk.httpclient.disableRetryConnect"))) {
            throw new IllegalArgumentException("Start Java with -Djdk.httpclient.disableRetryConnect=true");
        }
        if (!"false".equalsIgnoreCase(System.getProperty("jdk.httpclient.enableAllMethodRetry"))) {
            throw new IllegalArgumentException("Start Java with -Djdk.httpclient.enableAllMethodRetry=false");
        }
        this.endpoint = URI.create(required(properties, "hgbench.endpoint"));
        if (!("http".equals(endpoint.getScheme()) || "https".equals(endpoint.getScheme())) ||
            endpoint.getHost() == null || endpoint.getUserInfo() != null || endpoint.getQuery() != null ||
            endpoint.getFragment() != null) {
            throw new IllegalArgumentException("Explicit HTTP(S) endpoint must omit credentials, query and fragment");
        }
        this.alias = required(properties, "hgbench.alias");
        if (!alias.matches("[A-Za-z_][A-Za-z0-9_.-]{0,127}")) {
            throw new IllegalArgumentException("Invalid traversal-source alias");
        }
        this.timeoutMillis = positive(properties, "hgbench.timeoutMs", 3600000);
        this.maxResponseBytes = positive(properties, "hgbench.maxResponseBytes", Integer.MAX_VALUE - 8);
        int threads = positive(properties, "hgbench.httpThreads", 1024);
        String authEnv = properties.get("hgbench.authEnv");
        if (authEnv != null && !authEnv.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("authEnv must name an environment variable");
        }
        this.authorization = authEnv == null ? null : System.getenv(authEnv);
        if (authEnv != null && (authorization == null || authorization.isBlank())) {
            throw new IllegalArgumentException("Configured authorization environment variable is unset");
        }
        AtomicInteger ordinal = new AtomicInteger();
        this.executor = Executors.newFixedThreadPool(threads, task -> {
            Thread worker = new Thread(task, "hgbench-http-" + ordinal.incrementAndGet());
            worker.setDaemon(true);
            return worker;
        });
        this.client = HttpClient.newBuilder()
                                .connectTimeout(Duration.ofMillis(Math.min(timeoutMillis, 5000)))
                                .followRedirects(HttpClient.Redirect.NEVER)
                                .version(HttpClient.Version.HTTP_1_1)
                                .executor(executor).build();
    }

    public JsonNode execute(String script, int operationType, Map<String, Object> parameters) throws Exception {
        if (closed.get()) {
            throw new IOException("Transport closed");
        }
        byte[] body = mapper.writeValueAsBytes(Map.of("gremlin", script, "language", "gremlin-groovy",
                "aliases", Map.of("g", alias),
                "bindings", Map.of("operationType", operationType, "parameters", parameters)));
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofMillis(timeoutMillis))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (authorization != null) {
            builder.header("Authorization", authorization);
        }
        CompletableFuture<HttpResponse<byte[]>> future = client.sendAsync(builder.build(),
                ignored -> new BoundedBody(maxResponseBytes));
        pending.add(future);
        if (closed.get()) {
            future.cancel(true);
        }
        try {
            // This deadline includes the full body. HttpRequest.timeout alone must not bound headers only.
            HttpResponse<byte[]> response = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            if (response.statusCode() != 200) {
                throw new IOException("Gremlin HTTP status " + response.statusCode());
            }
            JsonNode envelope = mapper.readTree(response.body());
            JsonNode code = envelope.path("status").path("code");
            if (!code.isIntegralNumber() || !code.canConvertToInt() ||
                !(code.intValue() == 200 || code.intValue() == 204)) {
                throw new IOException("Gremlin result status is not successful");
            }
            JsonNode data = envelope.path("result").path("data");
            if (!data.isArray()) {
                throw new IOException("Expected complete untyped GraphSON result.data array");
            }
            return data;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw error;
        } finally {
            future.cancel(true);
            pending.remove(future);
        }
    }

    static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing " + key);
        }
        return value;
    }

    static int positive(Map<String, String> values, String key, int maximum) {
        int value = Integer.parseInt(required(values, key));
        if (value <= 0 || value > maximum) {
            throw new IllegalArgumentException("Out of range " + key);
        }
        return value;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            pending.forEach(future -> future.cancel(true));
            executor.shutdownNow();
        }
    }

    private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final int maximum;
        private Flow.Subscription subscription;

        BoundedBody(int maximum) {
            this.maximum = maximum;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            result.whenComplete((ignored, error) -> {
                if (error != null) {
                    value.cancel();
                }
            });
            value.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > maximum - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(new IOException("Response byte budget exceeded; result incomplete"));
                    return;
                }
                byte[] part = new byte[buffer.remaining()];
                buffer.get(part);
                bytes.write(part, 0, part.length);
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            result.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            result.complete(bytes.toByteArray());
        }
    }
}
