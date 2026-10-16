package org.operaton.bpm.extension.nifi.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"operaton", "bpmn", "external task", "workflow", "rest"})
@CapabilityDescription("Operaton External Task client using the Operaton REST API "
        + "(fetch and lock, complete, failure, BPMN error, extend lock, unlock).")
public class StandardOperatonExternalTaskService extends AbstractControllerService
        implements OperatonExternalTaskService {

    public static final PropertyDescriptor REST_URL = new PropertyDescriptor.Builder()
            .name("Operaton REST URL")
            .description("Base URL of the Operaton REST API, e.g. http://localhost:8080/engine-rest")
            .required(true)
            .defaultValue("http://localhost:8080/engine-rest")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor WORKER_ID = new PropertyDescriptor.Builder()
            .name("Worker ID")
            .description("Worker id used to lock external tasks. Tasks can only be completed "
                    + "or failed by the worker that locked them. Should be unique per NiFi node.")
            .required(true)
            .defaultValue("nifi-${hostname()}")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username for HTTP basic authentication")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password for HTTP basic authentication")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(USERNAME)
            .build();

    public static final PropertyDescriptor BEARER_TOKEN = new PropertyDescriptor.Builder()
            .name("Bearer Token")
            .description("Bearer token for HTTP OAuth2 / token authentication. Takes precedence over "
                    + "Username/Password when set.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Request Timeout")
            .description("Timeout for REST requests. Long polling timeouts of fetch and lock "
                    + "requests are added to this.")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    /** Maximum length of an error response body included in exception messages. */
    private static final int MAX_ERROR_LENGTH = 500;

    private static final List<PropertyDescriptor> PROPERTIES =
            List.of(REST_URL, WORKER_ID, USERNAME, PASSWORD, BEARER_TOKEN, REQUEST_TIMEOUT);

    private final ObjectMapper mapper = new ObjectMapper();

    private volatile HttpClient client;
    private volatile String baseUrl;
    private volatile String workerId;
    private volatile String authorization;
    private volatile Duration requestTimeout;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        String url = context.getProperty(REST_URL).evaluateAttributeExpressions().getValue();
        baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        workerId = context.getProperty(WORKER_ID).evaluateAttributeExpressions().getValue();
        requestTimeout = Duration.ofMillis(
                context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));

        String bearerToken = context.getProperty(BEARER_TOKEN).getValue();
        String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        if (bearerToken != null && !bearerToken.isBlank()) {
            authorization = "Bearer " + bearerToken.trim();
        } else if (username != null) {
            String password = context.getProperty(PASSWORD).getValue();
            String credentials = username + ":" + (password == null ? "" : password);
            authorization = "Basic " + Base64.getEncoder()
                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        } else {
            authorization = null;
        }

        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(requestTimeout)
                .build();
    }

    @OnDisabled
    public void onDisabled() {
        HttpClient httpClient = client;
        client = null;
        if (httpClient != null) {
            // close() would wait for in-flight (long polling) requests to finish
            httpClient.shutdownNow();
        }
    }

    @Override
    public String getWorkerId() {
        return workerId;
    }

    @Override
    public List<ExternalTask> fetchAndLock(String topicName, int maxTasks, long lockDuration,
                                           List<String> variables, long asyncResponseTimeout)
            throws OperatonException {
        return fetchAndLock(new FetchAndLockQuery(
                topicName, maxTasks, lockDuration, variables, asyncResponseTimeout));
    }

    @Override
    public List<ExternalTask> fetchAndLock(FetchAndLockQuery query) throws OperatonException {
        ObjectNode body = mapper.createObjectNode()
                .put("workerId", workerId)
                .put("maxTasks", query.maxTasks())
                .put("usePriority", query.usePriority());
        if (query.asyncResponseTimeout() > 0) {
            body.put("asyncResponseTimeout", query.asyncResponseTimeout());
        }
        ObjectNode topic = body.putArray("topics").addObject()
                .put("topicName", query.topicName())
                .put("lockDuration", query.lockDuration());
        if (query.variables() != null) {
            ArrayNode names = topic.putArray("variables");
            query.variables().forEach(names::add);
        }
        if (query.localVariables()) {
            topic.put("localVariables", true);
        }
        if (!query.deserializeValues()) {
            topic.put("deserializeValues", false);
        }
        if (query.includeExtensionProperties()) {
            topic.put("includeExtensionProperties", true);
        }
        if (query.businessKey() != null && !query.businessKey().isBlank()) {
            topic.put("businessKey", query.businessKey());
        }
        if (query.processDefinitionId() != null && !query.processDefinitionId().isBlank()) {
            topic.put("processDefinitionId", query.processDefinitionId());
        }
        if (query.processDefinitionIdIn() != null && !query.processDefinitionIdIn().isEmpty()) {
            ArrayNode ids = topic.putArray("processDefinitionIdIn");
            query.processDefinitionIdIn().forEach(ids::add);
        }
        if (query.processDefinitionKey() != null && !query.processDefinitionKey().isBlank()) {
            topic.put("processDefinitionKey", query.processDefinitionKey());
        }
        if (query.processDefinitionKeyIn() != null && !query.processDefinitionKeyIn().isEmpty()) {
            ArrayNode keys = topic.putArray("processDefinitionKeyIn");
            query.processDefinitionKeyIn().forEach(keys::add);
        }
        if (query.processDefinitionVersionTag() != null && !query.processDefinitionVersionTag().isBlank()) {
            topic.put("processDefinitionVersionTag", query.processDefinitionVersionTag());
        }
        if (query.withoutTenantId()) {
            topic.put("withoutTenantId", true);
        }
        if (query.tenantIdIn() != null && !query.tenantIdIn().isEmpty()) {
            ArrayNode tenants = topic.putArray("tenantIdIn");
            query.tenantIdIn().forEach(tenants::add);
        }
        putVariables(topic, "processVariables", query.processVariablesJson());

        JsonNode response = post("/external-task/fetchAndLock", body,
                requestTimeout.plusMillis(Math.max(query.asyncResponseTimeout(), 0)));

        List<ExternalTask> tasks = new ArrayList<>();
        if (response != null) {
            for (JsonNode task : response) {
                tasks.add(toExternalTask(task));
            }
        }
        return tasks;
    }

    @Override
    public void complete(String taskId, String variablesJson, String localVariablesJson)
            throws OperatonException {
        ObjectNode body = mapper.createObjectNode().put("workerId", workerId);
        putVariables(body, "variables", variablesJson);
        putVariables(body, "localVariables", localVariablesJson);
        post(taskPath(taskId, "complete"), body, requestTimeout);
    }

    @Override
    public void handleFailure(String taskId, String errorMessage, String errorDetails,
                              int retries, long retryTimeout) throws OperatonException {
        handleFailure(taskId, errorMessage, errorDetails, retries, retryTimeout, null, null);
    }

    @Override
    public void handleFailure(String taskId, String errorMessage, String errorDetails,
                              int retries, long retryTimeout,
                              String variablesJson, String localVariablesJson)
            throws OperatonException {
        ObjectNode body = mapper.createObjectNode()
                .put("workerId", workerId)
                .put("errorMessage", errorMessage)
                .put("errorDetails", errorDetails)
                .put("retries", Math.max(retries, 0))
                .put("retryTimeout", Math.max(retryTimeout, 0));
        putVariables(body, "variables", variablesJson);
        putVariables(body, "localVariables", localVariablesJson);
        post(taskPath(taskId, "failure"), body, requestTimeout);
    }

    @Override
    public void handleBpmnError(String taskId, String errorCode, String errorMessage,
                                String variablesJson) throws OperatonException {
        ObjectNode body = mapper.createObjectNode()
                .put("workerId", workerId)
                .put("errorCode", errorCode)
                .put("errorMessage", errorMessage);
        putVariables(body, "variables", variablesJson);
        post(taskPath(taskId, "bpmnError"), body, requestTimeout);
    }

    @Override
    public void extendLock(String taskId, long newDuration) throws OperatonException {
        ObjectNode body = mapper.createObjectNode()
                .put("workerId", workerId)
                .put("newDuration", newDuration);
        post(taskPath(taskId, "extendLock"), body, requestTimeout);
    }

    @Override
    public void unlock(String taskId) throws OperatonException {
        post(taskPath(taskId, "unlock"), mapper.createObjectNode(), requestTimeout);
    }

    private static String taskPath(String taskId, String action) throws OperatonException {
        if (taskId == null || taskId.isBlank()) {
            throw new OperatonException("External task id is missing", 0);
        }
        return "/external-task/" + URLEncoder.encode(taskId, StandardCharsets.UTF_8)
                + "/" + action;
    }

    private void putVariables(ObjectNode body, String field, String variablesJson)
            throws OperatonException {
        if (variablesJson == null || variablesJson.isBlank()) {
            return;
        }
        try {
            JsonNode variables = mapper.readTree(variablesJson);
            if (!variables.isObject()) {
                throw new OperatonException(field + " must be a JSON object", 0);
            }
            body.set(field, variables);
        } catch (JsonProcessingException e) {
            throw new OperatonException("Invalid " + field + " JSON: " + e.getOriginalMessage(), 0, e);
        }
    }

    /**
     * POST a JSON body and return the parsed response, or null for an empty response.
     */
    private JsonNode post(String path, JsonNode body, Duration timeout) throws OperatonException {
        HttpClient httpClient = client;
        if (httpClient == null) {
            throw new OperatonException("Controller service is not enabled", 0);
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
        if (authorization != null) {
            request.header("Authorization", authorization);
        }

        HttpResponse<String> response;
        try {
            response = httpClient.send(request.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new OperatonException("POST " + path + " failed: " + e.getMessage(), 0, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OperatonException("POST " + path + " was interrupted", 0, e);
        }

        int status = response.statusCode();
        String responseBody = response.body();
        if (status < 200 || status >= 300) {
            throw new OperatonException("POST " + path + " returned " + status + ": "
                    + errorMessage(responseBody), status);
        }
        if (responseBody == null || responseBody.isBlank()) {
            return null;
        }
        try {
            return mapper.readTree(responseBody);
        } catch (JsonProcessingException e) {
            throw new OperatonException("POST " + path + " returned invalid JSON", status, e);
        }
    }

    /**
     * Operaton REST errors are {"type": "...", "message": "..."}; fall back to the truncated raw body, e.g. a proxy error page.
     */
    private String errorMessage(String responseBody) {
        if (responseBody == null || responseBody.isBlank()) {
            return "(no response body)";
        }
        try {
            JsonNode message = mapper.readTree(responseBody).get("message");
            if (message != null && message.isTextual()) {
                return message.asText();
            }
        } catch (JsonProcessingException ignored) {
            // not JSON
        }
        return responseBody.length() <= MAX_ERROR_LENGTH
                ? responseBody : responseBody.substring(0, MAX_ERROR_LENGTH) + "...";
    }

    private static ExternalTask toExternalTask(JsonNode task) {
        JsonNode variables = task.get("variables");
        Map<String, String> extensionProperties = new LinkedHashMap<>();
        JsonNode extPropsNode = task.get("extensionProperties");
        if (extPropsNode != null && extPropsNode.isObject()) {
            extPropsNode.fields().forEachRemaining(entry -> {
                if (entry.getValue() != null && !entry.getValue().isNull()) {
                    extensionProperties.put(entry.getKey(), entry.getValue().asText());
                }
            });
        }
        return new ExternalTask(
                text(task, "id"),
                text(task, "topicName"),
                text(task, "workerId"),
                text(task, "activityId"),
                text(task, "activityInstanceId"),
                text(task, "executionId"),
                text(task, "processInstanceId"),
                text(task, "processDefinitionId"),
                text(task, "processDefinitionKey"),
                text(task, "processDefinitionVersionTag"),
                text(task, "businessKey"),
                text(task, "tenantId"),
                task.hasNonNull("retries") ? task.get("retries").asInt() : null,
                task.hasNonNull("priority") ? task.get("priority").asLong() : null,
                text(task, "createTime"),
                text(task, "lockExpirationTime"),
                text(task, "errorMessage"),
                text(task, "errorDetails"),
                variables == null || variables.isNull() ? "{}" : variables.toString(),
                extensionProperties);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }
}
