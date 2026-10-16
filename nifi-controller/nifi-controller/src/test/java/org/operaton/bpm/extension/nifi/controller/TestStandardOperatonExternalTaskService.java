package org.operaton.bpm.extension.nifi.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

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

    private final List<Request> requests = new ArrayList<>();
    private HttpServer server;
    private int responseStatus;
    private String responseBody;

    private TestRunner runner;
    private StandardOperatonExternalTaskService service;

    @BeforeEach
    public void setUp() throws IOException, InitializationException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/engine-rest", this::handle);
        server.start();
        responseStatus = 204;
        responseBody = "";

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
}
