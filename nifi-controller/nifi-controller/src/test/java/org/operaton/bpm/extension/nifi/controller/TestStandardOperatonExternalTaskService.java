package org.operaton.bpm.extension.nifi.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.net.Proxy;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestStandardOperatonExternalTaskService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A request received by the stub Operaton REST API. */
    private record Request(String path, String query, String authorization, JsonNode body) {
        Request(String path, String authorization, JsonNode body) {
            this(path, null, authorization, body);
        }
    }

    /** A request received by the stub OAuth2 token endpoint. */
    private record TokenRequest(String method, String contentType, String body) {
    }

    private final List<Request> requests = new ArrayList<>();
    private final List<TokenRequest> tokenRequests = new ArrayList<>();
    private HttpServer server;
    private volatile int responseStatus;
    private volatile String responseBody;
    /** Response statuses overriding {@link #responseStatus} by request path. */
    private final Map<String, Integer> responseStatusByPath = new ConcurrentHashMap<>();
    private int tokenResponseStatus;
    private String tokenResponseBody;

    private TestRunner runner;
    private StandardOperatonExternalTaskService service;

    @BeforeEach
    public void setUp() throws IOException, InitializationException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/engine-rest", this::handle);
        server.createContext("/oauth/token", exchange -> {
            byte[] in = exchange.getRequestBody().readAllBytes();
            synchronized (tokenRequests) {
                tokenRequests.add(new TokenRequest(exchange.getRequestMethod(),
                        exchange.getRequestHeaders().getFirst("Content-Type"),
                        new String(in, StandardCharsets.UTF_8)));
            }
            byte[] out = tokenResponseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(tokenResponseStatus, out.length == 0 ? -1 : out.length);
            if (out.length > 0) {
                exchange.getResponseBody().write(out);
            }
            exchange.close();
        });
        server.start();
        responseStatus = 204;
        responseBody = "";
        tokenResponseStatus = 200;
        tokenResponseBody = """
                {"access_token": "sa-token-1", "token_type": "Bearer", "expires_in": 3600}
                """;
        tokenRequests.clear();

        runner = TestRunners.newTestRunner(TestProcessor.class);
        service = new StandardOperatonExternalTaskService();
        // Tests extend locks at fake clock times
        service.setLockExtensionInterval(Duration.ofDays(1).toMillis());
        runner.addControllerService("operaton", service);
        runner.setProperty(service, StandardOperatonExternalTaskService.REST_URL,
                "http://127.0.0.1:" + server.getAddress().getPort() + "/engine-rest/");
        runner.setProperty(service, StandardOperatonExternalTaskService.WORKER_ID, "worker-1");
    }

    @AfterEach
    public void tearDown() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] in = exchange.getRequestBody().readAllBytes();
        synchronized (requests) {
            requests.add(new Request(exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getQuery(),
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    in.length == 0 ? null : MAPPER.readTree(in)));
        }
        byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(
                responseStatusByPath.getOrDefault(exchange.getRequestURI().getPath(), responseStatus),
                out.length == 0 ? -1 : out.length);
        if (out.length > 0) {
            exchange.getResponseBody().write(out);
        }
        exchange.close();
    }

    private Request lastRequest() {
        synchronized (requests) {
            return requests.get(requests.size() - 1);
        }
    }

    @Test
    public void testFetchAndLock() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 200;
        responseBody = """
                [{"id": "task-1", "topicName": "nifi", "workerId": "worker-1",
                  "processInstanceId": "pi-1", "businessKey": "bk", "retries": null,
                  "priority": 5, "variables": {"a": {"type": "String", "value": "x", "valueInfo": {}}}}]
                """;

        List<ExternalTask> tasks = service.fetchAndLock("nifi", 3, 60000, List.of("a"), 1000);

        assertEquals(1, tasks.size());
        ExternalTask task = tasks.get(0);
        assertEquals("task-1", task.id());
        assertEquals("pi-1", task.processInstanceId());
        assertEquals("bk", task.businessKey());
        assertNull(task.retries());
        assertEquals(5L, task.priority());
        assertEquals("x", MAPPER.readTree(task.variablesJson()).at("/a/value").asText());

        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/fetchAndLock", request.path());
        assertNull(request.authorization());
        assertEquals("worker-1", request.body().get("workerId").asText());
        assertEquals(3, request.body().get("maxTasks").asInt());
        assertEquals(1000, request.body().get("asyncResponseTimeout").asLong());
        JsonNode topic = request.body().at("/topics/0");
        assertEquals("nifi", topic.get("topicName").asText());
        assertEquals(60000, topic.get("lockDuration").asLong());
        assertEquals("a", topic.at("/variables/0").asText());
    }

    @Test
    public void testFetchAllVariablesWithoutLongPolling() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 200;
        responseBody = "[]";

        assertEquals(List.of(), service.fetchAndLock("nifi", 1, 1000, null, 0));

        JsonNode body = lastRequest().body();
        assertNull(body.get("asyncResponseTimeout"));
        assertNull(body.at("/topics/0").get("variables"));
    }

    @Test
    public void testComplete() throws Exception {
        runner.enableControllerService(service);

        service.complete("task-1", "{\"a\": {\"type\": \"Integer\", \"value\": 1}}", null);

        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/task-1/complete", request.path());
        assertEquals("worker-1", request.body().get("workerId").asText());
        assertEquals(1, request.body().at("/variables/a/value").asInt());
        assertNull(request.body().get("localVariables"));
    }

    @Test
    public void testHandleFailure() throws Exception {
        runner.enableControllerService(service);

        service.handleFailure("task-1", "boom", "stack", -1, 5000);

        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/task-1/failure", request.path());
        assertEquals("boom", request.body().get("errorMessage").asText());
        assertEquals("stack", request.body().get("errorDetails").asText());
        assertEquals(0, request.body().get("retries").asInt());
        assertEquals(5000, request.body().get("retryTimeout").asLong());
    }

    @Test
    public void testHandleBpmnError() throws Exception {
        runner.enableControllerService(service);

        service.handleBpmnError("task-1", "E1", "msg", null);

        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/task-1/bpmnError", request.path());
        assertEquals("E1", request.body().get("errorCode").asText());
        assertEquals("msg", request.body().get("errorMessage").asText());
    }

    @Test
    public void testUnlock() throws Exception {
        runner.enableControllerService(service);

        service.unlock("task-1");

        assertEquals("/engine-rest/external-task/task-1/unlock", lastRequest().path());
    }

    @Test
    public void testFetchAndLockEmptyResponse() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 200;

        assertEquals(List.of(), service.fetchAndLock("nifi", 1, 1000, null, 0));
    }

    @Test
    public void testDefaultWorkerIdIncludesHostname() {
        runner.removeProperty(service, StandardOperatonExternalTaskService.WORKER_ID);
        runner.enableControllerService(service);

        assertTrue(service.getWorkerId().startsWith("nifi-"));
        assertTrue(service.getWorkerId().length() > "nifi-".length());
    }

    @Test
    public void testBasicAuthentication() throws Exception {
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.BASIC.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.USERNAME, "demo");
        runner.setProperty(service, StandardOperatonExternalTaskService.PASSWORD, "secret");
        runner.enableControllerService(service);

        service.unlock("task-1");

        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("demo:secret".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, lastRequest().authorization());
    }

    @Test
    public void testBearerTokenAuthentication() throws Exception {
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.BEARER.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.BEARER_TOKEN, "my-jwt-token");
        runner.enableControllerService(service);

        service.unlock("task-1");

        assertEquals("Bearer my-jwt-token", lastRequest().authorization());
    }

    @Test
    public void testOAuth2ServiceAccountAuthentication() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "operaton-client");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "top-secret");
        runner.enableControllerService(service);

        service.unlock("task-1");

        // Verify Operaton request carries acquired Bearer token
        assertEquals("Bearer sa-token-1", lastRequest().authorization());

        // Verify token request parameters
        assertEquals(1, tokenRequests.size());
        TokenRequest tr = tokenRequests.get(0);
        assertEquals("POST", tr.method());
        assertTrue(tr.contentType().startsWith("application/x-www-form-urlencoded"));
        assertTrue(tr.body().contains("grant_type=client_credentials"));
        assertTrue(tr.body().contains("client_id=operaton-client"));
        assertTrue(tr.body().contains("client_secret=top-secret"));
    }

    @Test
    public void testOAuth2ServiceAccountWithScope() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-app");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret123");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_SCOPE, "tasks:manage engine:read");
        runner.enableControllerService(service);

        service.unlock("task-1");

        assertEquals("Bearer sa-token-1", lastRequest().authorization());
        assertEquals(1, tokenRequests.size());
        assertTrue(tokenRequests.get(0).body().contains("scope=tasks%3Amanage+engine%3Aread"));
    }

    @Test
    public void testOAuth2ServiceAccountTokenCachingAndRefresh() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-app");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret123");
        runner.enableControllerService(service);

        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-16T12:00:00Z"));
        // Token expires in 100 seconds (i.e. at 12:01:40), refresh window is 30s (triggers at or after 12:01:10)
        tokenResponseBody = "{\"access_token\": \"sa-token-1\", \"token_type\": \"Bearer\", \"expires_in\": 100}";

        OAuth2TokenManager customManager = new OAuth2TokenManager(
                java.net.http.HttpClient.newHttpClient(), tokenUrl, "client-app", "secret123",
                null, Duration.ofSeconds(5), MAPPER, now::get, Duration.ofSeconds(30));
        service.setTokenManager(customManager);

        // First call fetches token
        service.unlock("task-1");
        assertEquals("Bearer sa-token-1", lastRequest().authorization());
        assertEquals(1, tokenRequests.size());

        // Second call 10s later reuses cached token
        now.set(now.get().plusSeconds(10)); // 12:00:10
        service.unlock("task-1");
        assertEquals(1, tokenRequests.size());

        // Next call 70s later (12:01:20 >= 12:01:10) is within the 30s refresh window -> fetches new token
        now.set(now.get().plusSeconds(70));
        tokenResponseBody = "{\"access_token\": \"sa-token-2\", \"token_type\": \"Bearer\", \"expires_in\": 100}";
        service.unlock("task-1");
        assertEquals(2, tokenRequests.size());
        assertEquals("Bearer sa-token-2", lastRequest().authorization());
    }

    @Test
    public void testOAuth2ServiceAccountRetryOn401Unauthorized() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-app");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret123");
        runner.enableControllerService(service);

        java.util.concurrent.atomic.AtomicInteger tokenAttempt = new java.util.concurrent.atomic.AtomicInteger(0);
        server.removeContext("/oauth/token");
        server.createContext("/oauth/token", exchange -> {
            int ta = tokenAttempt.incrementAndGet();
            byte[] in = exchange.getRequestBody().readAllBytes();
            synchronized (tokenRequests) {
                tokenRequests.add(new TokenRequest(exchange.getRequestMethod(),
                        exchange.getRequestHeaders().getFirst("Content-Type"),
                        new String(in, StandardCharsets.UTF_8)));
            }
            String body = ta == 1
                    ? "{\"access_token\": \"token-old\", \"expires_in\": 3600}"
                    : "{\"access_token\": \"token-new\", \"expires_in\": 3600}";
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });

        // Configure Operaton to return 401 on the first attempt, then 204 on subsequent attempts
        server.removeContext("/engine-rest");
        java.util.concurrent.atomic.AtomicInteger attempt = new java.util.concurrent.atomic.AtomicInteger(0);
        server.createContext("/engine-rest", exchange -> {
            int currentAttempt = attempt.incrementAndGet();
            byte[] in = exchange.getRequestBody().readAllBytes();
            synchronized (requests) {
                requests.add(new Request(exchange.getRequestURI().getPath(),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        in.length == 0 ? null : MAPPER.readTree(in)));
            }
            if (currentAttempt == 1) {
                // Reject old token
                byte[] out = "{\"message\": \"The token is invalid or expired\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(401, out.length);
                exchange.getResponseBody().write(out);
            } else {
                exchange.sendResponseHeaders(204, -1);
            }
            exchange.close();
        });

        service.unlock("task-1");

        // Ensure 2 calls were made to token endpoint and 2 calls to Operaton API
        assertEquals(2, tokenRequests.size());
        assertEquals(2, requests.size());
        assertEquals("Bearer token-old", requests.get(0).authorization());
        assertEquals("Bearer token-new", requests.get(1).authorization());
    }

    @Test
    public void testOAuth2ServiceAccountValidation() {
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        // Only endpoint configured -> invalid
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.assertNotValid(service);

        // Endpoint and Client ID without Secret -> invalid
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-id");
        runner.assertNotValid(service);

        // All three configured -> valid
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret");
        runner.assertValid(service);
    }

    @Test
    public void testOAuth2ServiceAccountTokenEndpointError() {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "bad-client");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "wrong-secret");
        tokenResponseStatus = 401;
        tokenResponseBody = "{\"error\": \"invalid_client\"}";
        runner.enableControllerService(service);

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.unlock("task-1"));
        assertEquals(401, e.getStatusCode());
        assertTrue(e.getMessage().contains("invalid_client"));
    }

    @Test
    public void testOAuth2AccessTokenProviderIntegration() throws Exception {
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2-provider", provider);
        runner.enableControllerService(provider);

        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_PROVIDER.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2-provider");
        runner.enableControllerService(service);

        service.unlock("task-1");

        assertEquals("Bearer mock-provider-token", lastRequest().authorization());
    }

    @Test
    public void testOAuth2AccessTokenProviderRetryOn401() throws Exception {
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2-provider", provider);
        runner.enableControllerService(provider);

        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_PROVIDER.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2-provider");
        runner.enableControllerService(service);

        server.removeContext("/engine-rest");
        java.util.concurrent.atomic.AtomicInteger attempt = new java.util.concurrent.atomic.AtomicInteger(0);
        server.createContext("/engine-rest", exchange -> {
            int currentAttempt = attempt.incrementAndGet();
            byte[] in = exchange.getRequestBody().readAllBytes();
            synchronized (requests) {
                requests.add(new Request(exchange.getRequestURI().getPath(),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        in.length == 0 ? null : MAPPER.readTree(in)));
            }
            if (currentAttempt == 1) {
                byte[] out = "{\"message\": \"Unauthorized\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(401, out.length);
                exchange.getResponseBody().write(out);
            } else {
                exchange.sendResponseHeaders(204, -1);
            }
            exchange.close();
        });

        service.unlock("task-1");

        assertEquals(1, provider.getRefreshCount());
        assertEquals(2, requests.size());
        assertEquals("Bearer mock-provider-token", requests.get(0).authorization());
        assertEquals("Bearer refreshed-mock-provider-token", requests.get(1).authorization());
    }

    @Test
    public void testOAuth2RetryFailsWhenRefreshedTokenAlsoRejected() {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-app");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret123");
        runner.enableControllerService(service);

        // Operaton rejects all tokens with 401
        server.removeContext("/engine-rest");
        server.createContext("/engine-rest", exchange -> {
            byte[] in = exchange.getRequestBody().readAllBytes();
            synchronized (requests) {
                requests.add(new Request(exchange.getRequestURI().getPath(),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        in.length == 0 ? null : MAPPER.readTree(in)));
            }
            byte[] out = "{\"message\": \"Invalid credentials\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(401, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.unlock("task-1"));
        assertEquals(401, e.getStatusCode());
        // Must strictly stop after 1 retry (initial + 1 retry = 2 calls)
        assertEquals(2, requests.size());
    }

    @Test
    public void testAuthenticationStrategyValidation() {
        // Default NONE -> valid
        runner.assertValid(service);

        // BASIC: invalid without username, valid with username, valid with username + password
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.BASIC.getValue());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.USERNAME, "user");
        runner.assertValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.PASSWORD, "pass");
        runner.assertValid(service);

        // BEARER: invalid without token, valid with token
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.BEARER.getValue());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.BEARER_TOKEN, "token");
        runner.assertValid(service);

        // OAUTH2_PROVIDER: invalid without provider, valid with provider
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        try {
            runner.addControllerService("val-provider", provider);
            runner.enableControllerService(provider);
        } catch (InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_PROVIDER.getValue());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "val-provider");
        runner.assertValid(service);

        // OAUTH2_CLIENT_CREDENTIALS: invalid if missing any required property
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, "http://localhost/token");
        runner.assertNotValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client");
        runner.assertNotValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret");
        runner.assertValid(service);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_SCOPE, "read");
        runner.assertValid(service);
    }

    @Test
    public void testMigratePropertiesRemovesAutomaticLockExtension() {
        Map<String, String> originalProps = new HashMap<>();
        originalProps.put("Automatic Lock Extension", "true");
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertFalse(config.hasProperty("Automatic Lock Extension"));
    }

    @Test
    public void testMigratePropertiesOAuth2ProviderPrecedence() {
        Map<String, String> originalProps = new HashMap<>();
        originalProps.put("OAuth2 Access Token Provider", "provider-id");
        originalProps.put("OAuth2 Token Endpoint", "http://localhost/token");
        originalProps.put("OAuth2 Client ID", "client");
        originalProps.put("OAuth2 Client Secret", "secret");
        originalProps.put("Bearer Token", "bearer");
        originalProps.put("Username", "user");
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertEquals(AuthenticationStrategy.OAUTH2_PROVIDER.getValue(),
                config.getPropertyValue("Authentication Strategy").orElse(null));
    }

    @Test
    public void testMigratePropertiesOAuth2ClientCredentialsPrecedence() {
        Map<String, String> originalProps = new HashMap<>();
        originalProps.put("OAuth2 Token Endpoint", "http://localhost/token");
        originalProps.put("OAuth2 Client ID", "client");
        originalProps.put("OAuth2 Client Secret", "secret");
        originalProps.put("Bearer Token", "bearer");
        originalProps.put("Username", "user");
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertEquals(AuthenticationStrategy.OAUTH2_CLIENT_CREDENTIALS.getValue(),
                config.getPropertyValue("Authentication Strategy").orElse(null));
    }

    @Test
    public void testMigratePropertiesBearerPrecedence() {
        Map<String, String> originalProps = new HashMap<>();
        originalProps.put("Bearer Token", "bearer");
        originalProps.put("Username", "user");
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertEquals(AuthenticationStrategy.BEARER.getValue(),
                config.getPropertyValue("Authentication Strategy").orElse(null));
    }

    @Test
    public void testMigratePropertiesBasicPrecedence() {
        Map<String, String> originalProps = new HashMap<>();
        originalProps.put("Username", "user");
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertEquals(AuthenticationStrategy.BASIC.getValue(),
                config.getPropertyValue("Authentication Strategy").orElse(null));
    }

    @Test
    public void testMigratePropertiesNonePrecedence() {
        Map<String, String> originalProps = new HashMap<>();
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertEquals(AuthenticationStrategy.NONE.getValue(),
                config.getPropertyValue("Authentication Strategy").orElse(null));
    }

    @Test
    public void testMigratePropertiesDoesNotOverwriteExistingStrategy() {
        Map<String, String> originalProps = new HashMap<>();
        originalProps.put("Authentication Strategy", "NONE");
        originalProps.put("Username", "user");
        MockPropertyConfiguration config = new MockPropertyConfiguration(originalProps);

        service.migrateProperties(config);

        assertEquals(AuthenticationStrategy.NONE.getValue(),
                config.getPropertyValue("Authentication Strategy").orElse(null));
    }

    @Test
    public void testOAuth2AccessTokenProviderReturnsBlankTokenThrows() throws Exception {
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider() {
            @Override
            public AccessToken getAccessDetails() {
                return new AccessToken("", null, "Bearer", 3600L, null);
            }
        };
        runner.addControllerService("blank-provider", provider);
        runner.enableControllerService(provider);

        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2_PROVIDER.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "blank-provider");
        runner.enableControllerService(service);

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.unlock("task-1"));
        assertEquals(0, e.getStatusCode());
        assertTrue(e.getMessage().contains("empty access token"));
    }

    @Test
    public void testFetchAndLockExtendedQuery() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 200;
        responseBody = """
                [{"id": "task-2", "topicName": "orders", "workerId": "worker-1",
                  "processDefinitionId": "pd-1", "processDefinitionKey": "order-process",
                  "processDefinitionVersionTag": "v2", "createTime": "2026-10-16T08:00:00.000+0000",
                  "errorMessage": "prev error", "errorDetails": "prev stack",
                  "extensionProperties": {"retryPolicy": "exponential", "timeout": "30s"},
                  "variables": {}}]
                """;

        OperatonExternalTaskService.FetchAndLockQuery query =
                new OperatonExternalTaskService.FetchAndLockQuery(
                        "orders", 5, 30000L, List.of("orderId"), 2000L,
                        false, true, false, true,
                        "bk-42", "pd-1", List.of("pd-1", "pd-2"),
                        "order-process", List.of("order-process"), "v2",
                        true, List.of("tenant-a"),
                        "{\"status\": {\"type\": \"String\", \"value\": \"NEW\"}}");

        List<ExternalTask> tasks = service.fetchAndLock(query);

        assertEquals(1, tasks.size());
        ExternalTask task = tasks.get(0);
        assertEquals("task-2", task.id());
        assertEquals("v2", task.processDefinitionVersionTag());
        assertEquals("2026-10-16T08:00:00.000+0000", task.createTime());
        assertEquals("prev error", task.errorMessage());
        assertEquals("prev stack", task.errorDetails());
        assertEquals("exponential", task.extensionProperties().get("retryPolicy"));
        assertEquals("30s", task.extensionProperties().get("timeout"));

        JsonNode body = lastRequest().body();
        assertEquals(false, body.get("usePriority").asBoolean());
        JsonNode topic = body.at("/topics/0");
        assertEquals(true, topic.get("localVariables").asBoolean());
        assertEquals(false, topic.get("deserializeValues").asBoolean());
        assertEquals(true, topic.get("includeExtensionProperties").asBoolean());
        assertEquals("bk-42", topic.get("businessKey").asText());
        assertEquals("pd-1", topic.get("processDefinitionId").asText());
        assertEquals("pd-2", topic.at("/processDefinitionIdIn/1").asText());
        assertEquals("order-process", topic.get("processDefinitionKey").asText());
        assertEquals("order-process", topic.at("/processDefinitionKeyIn/0").asText());
        assertEquals("v2", topic.get("processDefinitionVersionTag").asText());
        assertEquals(true, topic.get("withoutTenantId").asBoolean());
        assertEquals("tenant-a", topic.at("/tenantIdIn/0").asText());
        assertEquals("NEW", topic.at("/processVariables/status/value").asText());
    }

    @Test
    public void testHandleFailureWithVariables() throws Exception {
        runner.enableControllerService(service);

        service.handleFailure("task-1", "boom", "stack", 2, 5000,
                "{\"v\": {\"type\": \"String\", \"value\": \"p\"}}",
                "{\"lv\": {\"type\": \"Integer\", \"value\": 9}}");

        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/task-1/failure", request.path());
        assertEquals("p", request.body().at("/variables/v/value").asText());
        assertEquals(9, request.body().at("/localVariables/lv/value").asInt());
    }

    @Test
    public void testErrorResponse() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 404;
        responseBody = "{\"type\": \"RestException\", \"message\": \"External task not found\"}";

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.complete("missing", null, null));

        assertEquals(404, e.getStatusCode());
        assertEquals("POST /external-task/missing/complete returned 404: External task not found",
                e.getMessage());
    }

    @Test
    public void testNonJsonErrorResponseIsTruncated() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 502;
        responseBody = "<html>" + "x".repeat(2000) + "</html>";

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.complete("task-1", null, null));

        assertEquals(502, e.getStatusCode());
        assertTrue(e.getMessage().endsWith("..."));
        assertTrue(e.getMessage().length() < 600);
    }

    @Test
    public void testInvalidVariables() {
        runner.enableControllerService(service);

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.complete("task-1", "[]", null));

        assertEquals(0, e.getStatusCode());
    }

    @Test
    public void testConnectionRefused() throws Exception {
        runner.enableControllerService(service);
        server.stop(0);

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.unlock("task-1"));

        assertEquals(0, e.getStatusCode());
    }

    private static final long T0 = 1_000_000_000L;
    private static final long LOCK_DURATION = 60_000;

    private final AtomicLong now = new AtomicLong(T0);

    private void enableWithFakeClock(String maxLockLifetime) {
        if (maxLockLifetime != null) {
            runner.setProperty(service, StandardOperatonExternalTaskService.MAX_LOCK_LIFETIME, maxLockLifetime);
        }
        service.setClock(now::get);
        runner.enableControllerService(service);
    }

    private void fetchTasks(String... taskIds) throws OperatonException {
        fetchTasks(LOCK_DURATION, taskIds);
    }

    private void fetchTasks(long lockDuration, String... taskIds) throws OperatonException {
        StringBuilder tasks = new StringBuilder("[");
        for (String taskId : taskIds) {
            tasks.append(tasks.length() > 1 ? "," : "")
                    .append("{\"id\": \"").append(taskId).append("\", \"topicName\": \"nifi\"}");
        }
        responseStatus = 200;
        responseBody = tasks.append("]").toString();
        service.fetchAndLock("nifi", taskIds.length, lockDuration, null, 0);
        responseStatus = 204;
        responseBody = "";
    }

    private List<Request> extendLockRequests() {
        synchronized (requests) {
            return requests.stream().filter(r -> r.path().endsWith("/extendLock")).toList();
        }
    }

    /** Advance the clock and extend due locks as the background thread would. */
    private void extendLocksAt(long time) {
        now.set(time);
        service.extendDueLocks(time);
    }

    @Test
    public void testAutomaticLockExtension() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1");

        extendLocksAt(T0 + LOCK_DURATION / 2 - 1);
        assertTrue(extendLockRequests().isEmpty());

        extendLocksAt(T0 + LOCK_DURATION / 2);
        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/task-1/extendLock", request.path());
        assertEquals("worker-1", request.body().get("workerId").asText());
        assertEquals(LOCK_DURATION, request.body().get("newDuration").asLong());
        assertEquals(T0 + LOCK_DURATION / 2 + LOCK_DURATION, service.getLeases().get("task-1").expiresAt());

        // Not due again until half way through the extended lock
        extendLocksAt(T0 + LOCK_DURATION - 1);
        assertEquals(1, extendLockRequests().size());
        extendLocksAt(T0 + LOCK_DURATION);
        assertEquals(2, extendLockRequests().size());
    }

    @Test
    public void testLocksAsLongAsMaxLockLifetimeAreNotExtended() throws Exception {
        enableWithFakeClock(null);
        fetchTasks(3_600_000, "task-1");

        assertEquals(0, service.getLeases().size());
    }

    @Test
    public void testFinishedTasksAreNoLongerExtended() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1", "task-2", "task-3", "task-4", "task-5");
        assertEquals(5, service.getLeases().size());

        service.complete("task-1", null, null);
        service.handleFailure("task-2", "boom", null, 1, 0);
        service.handleBpmnError("task-3", "E1", null, null);
        service.unlock("task-4");

        assertEquals(1, service.getLeases().size());
        extendLocksAt(T0 + LOCK_DURATION / 2);
        assertEquals(List.of("/engine-rest/external-task/task-5/extendLock"),
                extendLockRequests().stream().map(Request::path).toList());
    }

    @Test
    public void testFailedCompletionKeepsExtendingLock() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1");
        responseStatus = 500;

        assertThrows(OperatonException.class, () -> service.complete("task-1", null, null));

        assertNotNull(service.getLeases().get("task-1"));
    }

    @Test
    public void testMaxLockLifetime() throws Exception {
        enableWithFakeClock("90 sec");
        fetchTasks("task-1");

        // Extended only up to the max lifetime
        extendLocksAt(T0 + LOCK_DURATION / 2);
        assertEquals(90_000 - LOCK_DURATION / 2, lastRequest().body().get("newDuration").asLong());

        // Then left to expire
        extendLocksAt(T0 + 90_000 - LOCK_DURATION / 2);
        assertNull(service.getLeases().get("task-1"));
        extendLocksAt(T0 + 10 * LOCK_DURATION);
        assertEquals(1, extendLockRequests().size());
    }

    @Test
    public void testLostLockIsNoLongerExtended() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1");
        responseStatus = 404;
        responseBody = "{\"type\": \"InvalidRequestException\", \"message\": \"not found\"}";

        extendLocksAt(T0 + LOCK_DURATION / 2);

        assertNull(service.getLeases().get("task-1"));
        extendLocksAt(T0 + 10 * LOCK_DURATION);
        assertEquals(1, extendLockRequests().size());
    }

    @Test
    public void testUnavailableEngineRetriesExtension() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1");
        responseStatus = 503;

        extendLocksAt(T0 + LOCK_DURATION / 2);
        assertEquals(T0 + LOCK_DURATION, service.getLeases().get("task-1").expiresAt());

        responseStatus = 204;
        extendLocksAt(T0 + LOCK_DURATION / 2 + 1000);
        assertEquals(T0 + LOCK_DURATION / 2 + 1000 + LOCK_DURATION,
                service.getLeases().get("task-1").expiresAt());
    }

    @Test
    public void testFailingTaskDoesNotBlockOtherExtensions() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1");
        now.set(T0 + 1000);
        fetchTasks("task-2");
        responseStatusByPath.put("/engine-rest/external-task/task-1/extendLock", 403);

        extendLocksAt(T0 + 1000 + LOCK_DURATION / 2);

        assertEquals(List.of("/engine-rest/external-task/task-1/extendLock",
                        "/engine-rest/external-task/task-2/extendLock"),
                extendLockRequests().stream().map(Request::path).toList());
        assertEquals(T0 + LOCK_DURATION, service.getLeases().get("task-1").expiresAt());
        assertEquals(T0 + 1000 + LOCK_DURATION / 2 + LOCK_DURATION,
                service.getLeases().get("task-2").expiresAt());
    }

    @Test
    public void testUnavailableEngineEndsExtensionRound() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1", "task-2");
        responseStatus = 503;

        extendLocksAt(T0 + LOCK_DURATION / 2);

        assertEquals(1, extendLockRequests().size());
    }

    @Test
    public void testShortLocksAreNotExtended() throws Exception {
        enableWithFakeClock(null);

        fetchTasks(2000, "task-1");

        assertEquals(0, service.getLeases().size());
    }

    @Test
    public void testLeasesAreRestoredAfterRestart() throws Exception {
        enableWithFakeClock(null);
        fetchTasks("task-1");
        extendLocksAt(T0);

        runner.disableControllerService(service);
        assertEquals("60000,1000000000,1000060000",
                runner.getStateManager(service).getState(Scope.LOCAL).get("task-1"));
        assertEquals(0, service.getLeases().size());

        runner.enableControllerService(service);
        extendLocksAt(T0 + LOCK_DURATION / 2);
        assertEquals("/engine-rest/external-task/task-1/extendLock", lastRequest().path());
    }

    @Test
    public void testEnableAndRequestWithSSLContextService() throws Exception {
        MockSSLContextProvider sslProvider = new MockSSLContextProvider();
        runner.addControllerService("ssl-provider", sslProvider);
        runner.enableControllerService(sslProvider);
        runner.setProperty(service, StandardOperatonExternalTaskService.SSL_CONTEXT_SERVICE, "ssl-provider");
        runner.assertValid(service);
        runner.enableControllerService(service);

        responseStatus = 200;
        responseBody = "[]";
        List<ExternalTask> tasks = service.fetchAndLock("demo", 10, 60000L, null, 0L);
        assertNotNull(tasks);
        assertEquals(0, tasks.size());
    }

    @Test
    public void testEnableAndRequestWithProxyConfiguration() throws Exception {
        List<String> proxyRequests = new ArrayList<>();
        HttpServer proxyServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        proxyServer.createContext("/", exchange -> {
            synchronized (proxyRequests) {
                proxyRequests.add(exchange.getRequestURI().toString());
            }
            byte[] out = "[]".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });
        proxyServer.start();
        try {
            MockProxyConfigurationService proxyService = new MockProxyConfigurationService(
                    "127.0.0.1", proxyServer.getAddress().getPort(), null, null);
            runner.addControllerService("proxy-service", proxyService);
            runner.enableControllerService(proxyService);
            runner.setProperty(service, StandardOperatonExternalTaskService.PROXY_CONFIGURATION_SERVICE, "proxy-service");
            runner.assertValid(service);
            runner.enableControllerService(service);

            List<ExternalTask> tasks = service.fetchAndLock("demo", 10, 60000L, null, 0L);
            assertNotNull(tasks);
            assertFalse(proxyRequests.isEmpty(), "Request should have passed through proxy");
        } finally {
            proxyServer.stop(0);
        }
    }

    @Test
    public void testEnableAndRequestWithProxyAuthentication() throws Exception {
        List<String> proxyAuthHeaders = new ArrayList<>();
        HttpServer proxyServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        proxyServer.createContext("/", exchange -> {
            String proxyAuth = exchange.getRequestHeaders().getFirst("Proxy-Authorization");
            if (proxyAuth != null) {
                synchronized (proxyAuthHeaders) {
                    proxyAuthHeaders.add(proxyAuth);
                }
                byte[] out = "[]".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, out.length);
                exchange.getResponseBody().write(out);
            } else {
                exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"proxy\"");
                exchange.sendResponseHeaders(407, 0);
            }
            exchange.close();
        });
        proxyServer.start();
        try {
            MockProxyConfigurationService proxyService = new MockProxyConfigurationService(
                    "127.0.0.1", proxyServer.getAddress().getPort(), "proxyuser", "proxypass");
            runner.addControllerService("proxy-auth-service", proxyService);
            runner.enableControllerService(proxyService);
            runner.setProperty(service, StandardOperatonExternalTaskService.PROXY_CONFIGURATION_SERVICE, "proxy-auth-service");
            runner.assertValid(service);
            runner.enableControllerService(service);

            List<ExternalTask> tasks = service.fetchAndLock("demo", 10, 60000L, null, 0L);
            assertNotNull(tasks);
            assertFalse(proxyAuthHeaders.isEmpty(), "Proxy-Authorization header should have been sent to proxy");
            String expectedAuth = "Basic " + Base64.getEncoder().encodeToString("proxyuser:proxypass".getBytes(StandardCharsets.UTF_8));
            assertEquals(expectedAuth, proxyAuthHeaders.get(0));
        } finally {
            proxyServer.stop(0);
        }
    }

    public static class MockSSLContextProvider extends AbstractControllerService implements SSLContextProvider {
        @Override
        public SSLContext createContext() {
            try {
                return SSLContext.getDefault();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Optional<X509ExtendedKeyManager> createKeyManager() {
            return Optional.empty();
        }

        @Override
        public X509TrustManager createTrustManager() {
            return null;
        }
    }

    public static class MockProxyConfigurationService extends AbstractControllerService implements ProxyConfigurationService {
        private final String host;
        private final int port;
        private final String user;
        private final String password;

        public MockProxyConfigurationService(String host, int port, String user, String password) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }

        @Override
        public ProxyConfiguration getConfiguration() {
            ProxyConfiguration config = new ProxyConfiguration();
            config.setProxyType(Proxy.Type.HTTP);
            config.setProxyServerHost(host);
            config.setProxyServerPort(port);
            if (user != null) {
                config.setProxyUserName(user);
                config.setProxyUserPassword(password);
            }
            return config;
        }
    }

    public static class MockOAuth2AccessTokenProvider extends AbstractControllerService
            implements OAuth2AccessTokenProvider {
        private String token = "mock-provider-token";
        private int refreshCount = 0;

        private boolean throwOnGet = false;

        public void setToken(String token) {
            this.token = token;
        }

        public void setThrowOnGet(boolean throwOnGet) {
            this.throwOnGet = throwOnGet;
        }

        @Override
        public AccessToken getAccessDetails() {
            if (throwOnGet) {
                throw new IllegalStateException("Token service unavailable");
            }
            return new AccessToken(token, null, "Bearer", 3600L, null);
        }

        @Override
        public void refreshAccessDetails() {
            refreshCount++;
            token = "refreshed-mock-provider-token";
        }

        public int getRefreshCount() {
            return refreshCount;
        }
    }

    @Test
    public void testVerifySuccessful() {
        responseStatus = 200;
        responseBody = "{\"version\": \"1.0.0\"}";

        List<ConfigVerificationResult> results = runner.verify(service, Map.of());

        assertEquals(2, results.size());
        assertEquals("Authentication", results.get(0).getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());

        assertEquals("Operaton Connectivity", results.get(1).getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("version 1.0.0"));

        Request req = lastRequest();
        assertEquals("/engine-rest/version", req.path());
        assertNull(req.authorization());
    }

    @Test
    public void testVerifyWithBasicAuth() {
        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY,
                AuthenticationStrategy.BASIC.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.USERNAME, "demo");
        runner.setProperty(service, StandardOperatonExternalTaskService.PASSWORD, "secret");

        responseStatus = 200;
        responseBody = "{\"version\": \"7.20.0\"}";

        List<ConfigVerificationResult> results = runner.verify(service, Map.of());

        assertEquals(2, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("version 7.20.0"));

        Request req = lastRequest();
        assertEquals("/engine-rest/version", req.path());
        assertEquals("Basic " + Base64.getEncoder().encodeToString("demo:secret".getBytes(StandardCharsets.UTF_8)),
                req.authorization());
    }

    @Test
    public void testVerifyCredentialsRejected401() {
        responseStatus = 401;
        responseBody = "{\"type\": \"Unauthorized\", \"message\": \"Bad credentials\"}";

        List<ConfigVerificationResult> results = runner.verify(service, Map.of());

        assertEquals(2, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.FAILED, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("Credentials rejected by Operaton REST API (HTTP 401)"));
    }

    @Test
    public void testVerifyCredentialsRejected403() {
        responseStatus = 403;
        responseBody = "Forbidden";

        List<ConfigVerificationResult> results = runner.verify(service, Map.of());

        assertEquals(2, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.FAILED, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("Credentials rejected by Operaton REST API (HTTP 403)"));
    }

    @Test
    public void testVerifyConnectionRefused() throws Exception {
        int unusedPort;
        try (var socket = new java.net.ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }
        runner.setProperty(service, StandardOperatonExternalTaskService.REST_URL,
                "http://127.0.0.1:" + unusedPort + "/engine-rest");

        List<ConfigVerificationResult> results = runner.verify(service, Map.of());

        assertEquals(2, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.FAILED, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("Failed to connect to Operaton REST API"));
    }

    @Test
    public void testVerifyOAuth2ProviderSuccessAndFailure() throws Exception {
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2-provider", provider);
        runner.enableControllerService(provider);

        runner.setProperty(service, StandardOperatonExternalTaskService.AUTHENTICATION_STRATEGY,
                AuthenticationStrategy.OAUTH2_PROVIDER.getValue());
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2-provider");

        responseStatus = 200;
        responseBody = "{\"version\": \"1.0.0\"}";

        List<ConfigVerificationResult> results = runner.verify(service, Map.of());
        assertEquals(2, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertEquals("Bearer mock-provider-token", lastRequest().authorization());

        provider.setThrowOnGet(true);
        List<ConfigVerificationResult> failedResults = runner.verify(service, Map.of());
        assertEquals(2, failedResults.size());
        assertEquals(Outcome.FAILED, failedResults.get(0).getOutcome());
        assertTrue(failedResults.get(0).getExplanation().contains("Token service unavailable"));
        assertEquals(Outcome.SKIPPED, failedResults.get(1).getOutcome());
        assertTrue(failedResults.get(1).getExplanation().contains("Skipped due to authentication failure"));
    }

    @Test
    public void testCountTasks() throws Exception {
        runner.enableControllerService(service);
        responseStatus = 200;
        responseBody = "{\"count\": 42}";

        long count = service.countTasks("invoice-processing");
        assertEquals(42L, count);

        Request req = lastRequest();
        assertEquals("/engine-rest/external-task/count", req.path());
        assertNotNull(req.query());
        assertTrue(req.query().contains("topicName=invoice-processing"));
        assertTrue(req.query().contains("notLocked=true"));
    }

    @Test
    public void testCountTasksEmptyTopicThrows() throws Exception {
        runner.enableControllerService(service);
        assertThrows(OperatonException.class, () -> service.countTasks(""));
        assertThrows(OperatonException.class, () -> service.countTasks(null));
    }
}
