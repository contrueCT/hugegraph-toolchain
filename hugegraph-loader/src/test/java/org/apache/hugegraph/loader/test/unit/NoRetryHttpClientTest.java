/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.loader.test.unit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.loader.util.NoRetryHttpClient;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class NoRetryHttpClientTest {

    private HttpServer server;
    private String url;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger redirected = new AtomicInteger();
    private final List<OkHttpClient> clients = new ArrayList<>();

    @Before
    public void setup() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/", exchange -> {
            this.requests.incrementAndGet();
            Assert.assertEquals("{}", new String(exchange.getRequestBody().readAllBytes(),
                                                StandardCharsets.UTF_8));
            String path = exchange.getRequestURI().getPath();
            if ("/lost".equals(path)) {
                // The request has been consumed; close without an acknowledgement.
                exchange.close();
                return;
            }
            int status = 200;
            if ("/unavailable".equals(path)) {
                status = 503;
                exchange.getResponseHeaders().add("Retry-After", "0");
            } else if ("/redirect".equals(path)) {
                status = 307;
                exchange.getResponseHeaders().add("Location", this.url + "/sink");
            } else if ("/sink".equals(path)) {
                this.redirected.incrementAndGet();
            }
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        this.server.start();
        this.url = "http://127.0.0.1:" + this.server.getAddress().getPort();
    }

    @After
    public void cleanup() {
        for (OkHttpClient client : this.clients) {
            client.dispatcher().executorService().shutdownNow();
            client.connectionPool().evictAll();
        }
        if (this.server != null) {
            this.server.stop(0);
        }
    }

    private OkHttpClient client(boolean strict) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(2, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS);
        if (strict) {
            NoRetryHttpClient.configure(builder);
        }
        OkHttpClient client = builder.build();
        this.clients.add(client);
        return client;
    }

    private int post(OkHttpClient client, String path) throws IOException {
        Request request = new Request.Builder().url(this.url + path)
                .post(RequestBody.create(MediaType.parse("application/json"), "{}")).build();
        try (Response response = client.newCall(request).execute()) {
            Assert.assertEquals("{}", response.body().string());
            return response.code();
        }
    }

    @Test
    public void testServerRequestedRetryDoesNotResendTheWrite() throws IOException {
        Assert.assertEquals(503, this.post(this.client(false), "/unavailable"));
        Assert.assertEquals(2, this.requests.get());
        this.requests.set(0);
        try {
            Assert.assertEquals(503, this.post(this.client(true), "/unavailable"));
        } catch (IOException error) {
            Assert.assertTrue(error.getMessage().contains("HTTP replay disabled"));
        }
        Assert.assertEquals(1, this.requests.get());
    }

    @Test
    public void testRedirectIsNotFollowed() throws IOException {
        Assert.assertEquals(307, this.post(this.client(true), "/redirect"));
        Assert.assertEquals(1, this.requests.get());
        Assert.assertEquals(0, this.redirected.get());
    }

    @Test
    public void testLostAcknowledgementIsNotRetried() {
        Assert.assertThrows(IOException.class, () -> this.post(this.client(true), "/lost"));
        Assert.assertEquals(1, this.requests.get());
    }

    @Test
    public void testIndependentCallsHaveIndependentAttemptCounters() throws IOException {
        OkHttpClient client = this.client(true);
        Assert.assertEquals(200, this.post(client, "/ok"));
        Assert.assertEquals(200, this.post(client, "/ok"));
        Assert.assertEquals(2, this.requests.get());
    }
}
