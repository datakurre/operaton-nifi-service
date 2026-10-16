package org.operaton.bpm.extension.nifi.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OAuth2TokenManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private int responseStatus;
    private String responseBody;
    private final List<String> receivedBodies = new ArrayList<>();
    private final AtomicInteger requestCount = new AtomicInteger(0);

    @BeforeEach
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", this::handleTokenRequest);
        server.start();

        responseStatus = 200;
        responseBody = "{\"access_token\": \"token-123\", \"token_type\": \"Bearer\", \"expires_in\": 3600}";
        receivedBodies.clear();
        requestCount.set(0);
    }

    @AfterEach
    public void tearDown() {
        server.stop(0);
    }

    private void handleTokenRequest(HttpExchange exchange) throws IOException {
        requestCount.incrementAndGet();
        byte[] in = exchange.getRequestBody().readAllBytes();
        synchronized (receivedBodies) {
            receivedBodies.add(new String(in, StandardCharsets.UTF_8));
        }
        byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(responseStatus, out.length == 0 ? -1 : out.length);
        if (out.length > 0) {
            exchange.getResponseBody().write(out);
        }
        exchange.close();
    }

    private String tokenUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/token";
    }

    @Test
    public void testTokenAcquisitionAndConcurrentCaching() throws Exception {
        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", "scope1",
                Duration.ofSeconds(5), MAPPER);

        int threadCount = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(pool.submit(() -> {
                startLatch.await();
                return manager.getAccessToken();
            }));
        }

        startLatch.countDown();
        for (Future<String> f : futures) {
            assertEquals("token-123", f.get());
        }
        pool.shutdown();

        // Exactly one HTTP call should have been made despite concurrent access
        assertEquals(1, requestCount.get());
        assertEquals("token-123", manager.getAccessToken());
        assertEquals(1, requestCount.get());
    }

    @Test
    public void testAdaptiveRefreshWindowForShortExpiresIn() throws Exception {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.parse("2026-10-16T10:00:00Z"));
        // expires_in = 20s is shorter than default 30s refresh window
        responseBody = "{\"access_token\": \"short-token\", \"expires_in\": 20}";

        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", null,
                Duration.ofSeconds(5), MAPPER, clock::get, Duration.ofSeconds(30));

        // Initial acquisition
        assertEquals("short-token", manager.getAccessToken());
        assertEquals(1, requestCount.get());

        // Token must remain cached immediately after acquisition (effective window is 20/2 = 10s)
        assertFalse(manager.isExpiring());

        // At +9 seconds: still within valid cached window
        clock.set(clock.get().plusSeconds(9));
        assertFalse(manager.isExpiring());
        assertEquals("short-token", manager.getAccessToken());
        assertEquals(1, requestCount.get());

        // At +11 seconds (from start): into the adaptive 10s refresh window -> refresh triggers
        clock.set(clock.get().plusSeconds(2));
        assertTrue(manager.isExpiring());
        responseBody = "{\"access_token\": \"refreshed-token\", \"expires_in\": 20}";
        assertEquals("refreshed-token", manager.getAccessToken());
        assertEquals(2, requestCount.get());
    }

    @Test
    public void testConditionalInvalidation() throws Exception {
        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", null,
                Duration.ofSeconds(5), MAPPER);

        assertEquals("token-123", manager.getAccessToken());
        assertEquals(1, requestCount.get());

        // Invalidate mismatching token (e.g. from an earlier stale 401 response) -> should not invalidate
        manager.invalidate("stale-token");
        assertEquals("token-123", manager.getAccessToken());
        assertEquals(1, requestCount.get());

        // Invalidate matching token -> clears cache
        manager.invalidate("token-123");
        responseBody = "{\"access_token\": \"token-456\", \"expires_in\": 3600}";
        assertEquals("token-456", manager.getAccessToken());
        assertEquals(2, requestCount.get());
    }

    @Test
    public void testLargeExpiresInOverflowProtection() throws Exception {
        // Very large number that would overflow Instant if unchecked
        responseBody = "{\"access_token\": \"infinite-token\", \"expires_in\": 999999999999999}";

        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", null,
                Duration.ofSeconds(5), MAPPER);

        assertEquals("infinite-token", manager.getAccessToken());
        assertFalse(manager.isExpiring());
    }

    @Test
    public void testTokenTypeValidation() throws Exception {
        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", null,
                Duration.ofSeconds(5), MAPPER);

        // Unsupported token type
        responseBody = "{\"access_token\": \"tok\", \"token_type\": \"MAC\", \"expires_in\": 3600}";
        OperatonException e = assertThrows(OperatonException.class, manager::getAccessToken);
        assertTrue(e.getMessage().contains("Unsupported OAuth2 token_type: MAC"));

        // Case-insensitive "bearer" succeeds
        responseBody = "{\"access_token\": \"tok2\", \"token_type\": \"bearer\", \"expires_in\": 3600}";
        assertEquals("tok2", manager.getAccessToken());
    }

    @Test
    public void testErrorBodyTruncation() {
        responseStatus = 502;
        responseBody = "<html>" + "Error".repeat(200) + "</html>";

        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", null,
                Duration.ofSeconds(5), MAPPER);

        OperatonException e = assertThrows(OperatonException.class, manager::getAccessToken);
        assertEquals(502, e.getStatusCode());
        assertTrue(e.getMessage().endsWith("..."));
        assertTrue(e.getMessage().length() < 600);
    }

    @Test
    public void testMissingAccessTokenThrows() {
        responseBody = "{\"expires_in\": 3600}";

        OAuth2TokenManager manager = new OAuth2TokenManager(
                HttpClient.newHttpClient(), tokenUrl(), "client", "secret", null,
                Duration.ofSeconds(5), MAPPER);

        OperatonException e = assertThrows(OperatonException.class, manager::getAccessToken);
        assertTrue(e.getMessage().contains("missing access_token"));
    }
}
