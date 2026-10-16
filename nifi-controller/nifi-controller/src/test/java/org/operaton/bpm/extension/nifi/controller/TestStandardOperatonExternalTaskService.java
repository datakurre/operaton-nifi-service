package org.operaton.bpm.extension.nifi.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestStandardOperatonExternalTaskService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A request received by the stub Operaton REST API. */
    private record Request(String path, String authorization, JsonNode body) {
    }

    /** A request received by the stub OAuth2 token endpoint. */
    private record TokenRequest(String method, String contentType, String body) {
    }

    private final List<Request> requests = new ArrayList<>();
    private final List<TokenRequest> tokenRequests = new ArrayList<>();
    private HttpServer server;
    private int responseStatus;
    private String responseBody;
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
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    in.length == 0 ? null : MAPPER.readTree(in)));
        }
        byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(responseStatus, out.length == 0 ? -1 : out.length);
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
    public void testExtendLock() throws Exception {
        runner.enableControllerService(service);

        service.extendLock("task-1", 30000);

        Request request = lastRequest();
        assertEquals("/engine-rest/external-task/task-1/extendLock", request.path());
        assertEquals(30000, request.body().get("newDuration").asLong());
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
        runner.setProperty(service, StandardOperatonExternalTaskService.USERNAME, "demo");
        runner.setProperty(service, StandardOperatonExternalTaskService.PASSWORD, "secret");
        runner.enableControllerService(service);

        service.extendLock("task-1", 1000);

        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("demo:secret".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, lastRequest().authorization());
    }

    @Test
    public void testBearerTokenAuthentication() throws Exception {
        runner.setProperty(service, StandardOperatonExternalTaskService.BEARER_TOKEN, "my-jwt-token");
        runner.enableControllerService(service);

        service.extendLock("task-1", 1000);

        assertEquals("Bearer my-jwt-token", lastRequest().authorization());
    }

    @Test
    public void testOAuth2ServiceAccountAuthentication() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "operaton-client");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "top-secret");
        runner.enableControllerService(service);

        service.extendLock("task-1", 1000);

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
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-app");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret123");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_SCOPE, "tasks:manage engine:read");
        runner.enableControllerService(service);

        service.extendLock("task-1", 1000);

        assertEquals("Bearer sa-token-1", lastRequest().authorization());
        assertEquals(1, tokenRequests.size());
        assertTrue(tokenRequests.get(0).body().contains("scope=tasks%3Amanage+engine%3Aread"));
    }

    @Test
    public void testOAuth2ServiceAccountTokenCachingAndRefresh() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
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
        service.extendLock("task-1", 1000);
        assertEquals("Bearer sa-token-1", lastRequest().authorization());
        assertEquals(1, tokenRequests.size());

        // Second call 10s later reuses cached token
        now.set(now.get().plusSeconds(10)); // 12:00:10
        service.extendLock("task-1", 1000);
        assertEquals(1, tokenRequests.size());

        // Next call 70s later (12:01:20 >= 12:01:10) is within the 30s refresh window -> fetches new token
        now.set(now.get().plusSeconds(70));
        tokenResponseBody = "{\"access_token\": \"sa-token-2\", \"token_type\": \"Bearer\", \"expires_in\": 100}";
        service.extendLock("task-1", 1000);
        assertEquals(2, tokenRequests.size());
        assertEquals("Bearer sa-token-2", lastRequest().authorization());
    }

    @Test
    public void testOAuth2ServiceAccountRetryOn401Unauthorized() throws Exception {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
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

        service.extendLock("task-1", 1000);

        // Ensure 2 calls were made to token endpoint and 2 calls to Operaton API
        assertEquals(2, tokenRequests.size());
        assertEquals(2, requests.size());
        assertEquals("Bearer token-old", requests.get(0).authorization());
        assertEquals("Bearer token-new", requests.get(1).authorization());
    }

    @Test
    public void testOAuth2ServiceAccountValidation() {
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
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "bad-client");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "wrong-secret");
        tokenResponseStatus = 401;
        tokenResponseBody = "{\"error\": \"invalid_client\"}";
        runner.enableControllerService(service);

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.extendLock("task-1", 1000));
        assertEquals(401, e.getStatusCode());
        assertTrue(e.getMessage().contains("invalid_client"));
    }

    @Test
    public void testOAuth2AccessTokenProviderIntegration() throws Exception {
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2-provider", provider);
        runner.enableControllerService(provider);

        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2-provider");
        runner.enableControllerService(service);

        service.extendLock("task-1", 1000);

        assertEquals("Bearer mock-provider-token", lastRequest().authorization());
    }

    @Test
    public void testOAuth2AccessTokenProviderRetryOn401() throws Exception {
        MockOAuth2AccessTokenProvider provider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2-provider", provider);
        runner.enableControllerService(provider);

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

        service.extendLock("task-1", 1000);

        assertEquals(1, provider.getRefreshCount());
        assertEquals(2, requests.size());
        assertEquals("Bearer mock-provider-token", requests.get(0).authorization());
        assertEquals("Bearer refreshed-mock-provider-token", requests.get(1).authorization());
    }

    @Test
    public void testOAuth2RetryFailsWhenRefreshedTokenAlsoRejected() {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
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
                () -> service.extendLock("task-1", 1000));
        assertEquals(401, e.getStatusCode());
        // Must strictly stop after 1 retry (initial + 1 retry = 2 calls)
        assertEquals(2, requests.size());
    }

    @Test
    public void testAuthenticationMutualExclusivityValidation() {
        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";

        // Direct OAuth2 + Bearer Token -> invalid
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT, tokenUrl);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID, "client-id");
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET, "secret");
        runner.setProperty(service, StandardOperatonExternalTaskService.BEARER_TOKEN, "static-token");
        runner.assertNotValid(service);

        // Bearer Token + Basic Auth -> invalid
        runner.removeProperty(service, StandardOperatonExternalTaskService.OAUTH2_TOKEN_ENDPOINT);
        runner.removeProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_ID);
        runner.removeProperty(service, StandardOperatonExternalTaskService.OAUTH2_CLIENT_SECRET);
        runner.setProperty(service, StandardOperatonExternalTaskService.USERNAME, "user");
        runner.setProperty(service, StandardOperatonExternalTaskService.PASSWORD, "pass");
        runner.assertNotValid(service);

        // Password without Username -> invalid
        runner.removeProperty(service, StandardOperatonExternalTaskService.BEARER_TOKEN);
        runner.removeProperty(service, StandardOperatonExternalTaskService.USERNAME);
        runner.assertNotValid(service);

        // Scope without Endpoint -> invalid
        runner.removeProperty(service, StandardOperatonExternalTaskService.PASSWORD);
        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_SCOPE, "read");
        runner.assertNotValid(service);
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

        runner.setProperty(service, StandardOperatonExternalTaskService.OAUTH2_ACCESS_TOKEN_PROVIDER, "blank-provider");
        runner.enableControllerService(service);

        OperatonException e = assertThrows(OperatonException.class,
                () -> service.extendLock("task-1", 1000));
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
                () -> service.extendLock("task-1", 1000));

        assertEquals(0, e.getStatusCode());
    }

    public static class MockOAuth2AccessTokenProvider extends AbstractControllerService
            implements OAuth2AccessTokenProvider {
        private String token = "mock-provider-token";
        private int refreshCount = 0;

        @Override
        public AccessToken getAccessDetails() {
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
}
