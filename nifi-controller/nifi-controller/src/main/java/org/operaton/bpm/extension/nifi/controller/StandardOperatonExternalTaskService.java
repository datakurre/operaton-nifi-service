package org.operaton.bpm.extension.nifi.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"operaton", "bpmn", "external task", "workflow", "rest", "oauth2"})
@CapabilityDescription("Operaton External Task client using the Operaton REST API "
        + "(fetch and lock, complete, failure, BPMN error, unlock). "
        + "Supports dynamic OAuth2 service account (client_credentials), OAuth2 Access Token Provider, "
        + "static Bearer token, and HTTP Basic authentication. Extends the locks of fetched "
        + "tasks automatically until they are completed, failed or unlocked.")
@Stateful(scopes = Scope.LOCAL, description = "The external tasks locked by this node and their lock "
        + "durations, so that their locks are still extended after a restart.")
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

    public static final PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .description("Optional controller service providing OAuth2 access tokens. If set, takes precedence "
                    + "over direct service account properties.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(false)
            .build();

    public static final PropertyDescriptor OAUTH2_TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("OAuth2 Token Endpoint")
            .description("OAuth2 Token Endpoint URL for service account authentication via client_credentials grant, "
                    + "e.g. https://auth.example.com/oauth/token. When configured along with Client ID and Secret, "
                    + "tokens are automatically fetched, cached, and refreshed before expiration.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor OAUTH2_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("OAuth2 Client ID")
            .description("OAuth2 Client ID for service account authentication")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(OAUTH2_TOKEN_ENDPOINT)
            .build();

    public static final PropertyDescriptor OAUTH2_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("OAuth2 Client Secret")
            .description("OAuth2 Client Secret for service account authentication")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(OAUTH2_CLIENT_ID)
            .build();

    public static final PropertyDescriptor OAUTH2_SCOPE = new PropertyDescriptor.Builder()
            .name("OAuth2 Scope")
            .description("Optional scope(s) for OAuth2 client_credentials token requests")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(OAUTH2_TOKEN_ENDPOINT)
            .build();

    public static final PropertyDescriptor BEARER_TOKEN = new PropertyDescriptor.Builder()
            .name("Bearer Token")
            .description("Fixed Bearer token for HTTP OAuth2 / token authentication. Used when OAuth2 "
                    + "service account / provider is not configured; takes precedence over Username/Password.")
            .required(false)
            .sensitive(true)
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
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(USERNAME)
            .build();

    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Request Timeout")
            .description("Timeout for REST requests. Long polling timeouts of fetch and lock "
                    + "requests are added to this.")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_LOCK_LIFETIME = new PropertyDescriptor.Builder()
            .name("Max Lock Lifetime")
            .description("How long after fetching locks are extended at most. Bounds how long a task "
                    + "stays locked when its FlowFile is dropped without finishing the task, "
                    + "e.g. by an auto-terminated relationship.")
            .required(true)
            .defaultValue("1 hour")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    /** Maximum length of an error response body included in exception messages. */
    private static final int MAX_ERROR_LENGTH = 500;

    /** How often locks due for extension are looked for. */
    private static final long LOCK_EXTENSION_INTERVAL_MILLIS = 1000;

    /** Shorter locks could expire between two rounds of extension. */
    private static final long MIN_EXTENDED_LOCK_DURATION_MILLIS = 3 * LOCK_EXTENSION_INTERVAL_MILLIS;

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            REST_URL,
            WORKER_ID,
            OAUTH2_ACCESS_TOKEN_PROVIDER,
            OAUTH2_TOKEN_ENDPOINT,
            OAUTH2_CLIENT_ID,
            OAUTH2_CLIENT_SECRET,
            OAUTH2_SCOPE,
            BEARER_TOKEN,
            USERNAME,
            PASSWORD,
            REQUEST_TIMEOUT,
            MAX_LOCK_LIFETIME);

    private final ObjectMapper mapper = new ObjectMapper();

    private volatile HttpClient client;
    private volatile String baseUrl;
    private volatile String workerId;
    private volatile Duration requestTimeout;

    private volatile OAuth2AccessTokenProvider oauth2Provider;
    private volatile OAuth2TokenManager tokenManager;
    private volatile String staticAuthorization;

    private final ExternalTaskLeases leases = new ExternalTaskLeases();
    private volatile LongSupplier clock = System::currentTimeMillis;
    private volatile long maxLockLifetime;
    private volatile ScheduledExecutorService lockExtender;
    private volatile boolean extending;
    private volatile long lockExtensionInterval = LOCK_EXTENSION_INTERVAL_MILLIS;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final boolean hasEndpoint = validationContext.getProperty(OAUTH2_TOKEN_ENDPOINT).isSet();
        final boolean hasClientId = validationContext.getProperty(OAUTH2_CLIENT_ID).isSet();
        final boolean hasClientSecret = validationContext.getProperty(OAUTH2_CLIENT_SECRET).isSet();
        final boolean hasScope = validationContext.getProperty(OAUTH2_SCOPE).isSet();

        final boolean hasProvider = validationContext.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).isSet();
        final boolean hasDirectOAuth2 = hasEndpoint || hasClientId || hasClientSecret;
        final boolean hasBearer = validationContext.getProperty(BEARER_TOKEN).isSet();
        final boolean hasUsername = validationContext.getProperty(USERNAME).isSet();
        final boolean hasPassword = validationContext.getProperty(PASSWORD).isSet();

        if (hasEndpoint || hasClientId || hasClientSecret) {
            if (!hasEndpoint || !hasClientId || !hasClientSecret) {
                results.add(new ValidationResult.Builder()
                        .subject("OAuth2 Service Account")
                        .valid(false)
                        .explanation("OAuth2 Token Endpoint, OAuth2 Client ID, and OAuth2 Client Secret must all be configured together.")
                        .build());
            }
        }

        if (hasScope && !hasEndpoint) {
            results.add(new ValidationResult.Builder()
                    .subject("OAuth2 Scope")
                    .valid(false)
                    .explanation("OAuth2 Scope cannot be configured without OAuth2 Token Endpoint.")
                    .build());
        }

        if (hasPassword && !hasUsername) {
            results.add(new ValidationResult.Builder()
                    .subject("Password")
                    .valid(false)
                    .explanation("Password cannot be configured without Username.")
                    .build());
        }

        int authCount = (hasProvider ? 1 : 0)
                + (hasDirectOAuth2 ? 1 : 0)
                + (hasBearer ? 1 : 0)
                + ((hasUsername || hasPassword) ? 1 : 0);

        if (authCount > 1) {
            results.add(new ValidationResult.Builder()
                    .subject("Authentication Configuration")
                    .valid(false)
                    .explanation("Only one authentication method may be configured: OAuth2 Access Token Provider, "
                            + "Direct OAuth2 Service Account, Bearer Token, or HTTP Basic Authentication.")
                    .build());
        }

        return results;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        // Extension is always automatic now, see Lock Extension in the README
        config.removeProperty("Automatic Lock Extension");
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        // In case a previous enabling failed without the service being disabled
        stopLockExtension();
        String url = context.getProperty(REST_URL).evaluateAttributeExpressions().getValue();
        baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        workerId = context.getProperty(WORKER_ID).evaluateAttributeExpressions().getValue();
        requestTimeout = Duration.ofMillis(
                context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));

        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(requestTimeout)
                .build();

        PropertyValue oauth2ServiceProp = context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER);
        if (oauth2ServiceProp != null && oauth2ServiceProp.isSet()) {
            oauth2Provider = oauth2ServiceProp.asControllerService(OAuth2AccessTokenProvider.class);
            tokenManager = null;
            staticAuthorization = null;
        } else {
            oauth2Provider = null;
            String tokenEndpoint = context.getProperty(OAUTH2_TOKEN_ENDPOINT).evaluateAttributeExpressions().getValue();
            String clientId = context.getProperty(OAUTH2_CLIENT_ID).evaluateAttributeExpressions().getValue();
            String clientSecret = context.getProperty(OAUTH2_CLIENT_SECRET).evaluateAttributeExpressions().getValue();
            String scope = context.getProperty(OAUTH2_SCOPE).evaluateAttributeExpressions().getValue();

            if (tokenEndpoint != null && !tokenEndpoint.isBlank()
                    && clientId != null && !clientId.isBlank()
                    && clientSecret != null && !clientSecret.isBlank()) {
                tokenManager = new OAuth2TokenManager(client, tokenEndpoint.trim(), clientId.trim(),
                        clientSecret, scope, requestTimeout, mapper);
                staticAuthorization = null;
            } else {
                tokenManager = null;
                String bearerToken = context.getProperty(BEARER_TOKEN).evaluateAttributeExpressions().getValue();
                String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
                if (bearerToken != null && !bearerToken.isBlank()) {
                    staticAuthorization = "Bearer " + bearerToken.trim();
                } else if (username != null) {
                    String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
                    String credentials = username + ":" + (password == null ? "" : password);
                    staticAuthorization = "Basic " + Base64.getEncoder()
                            .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                } else {
                    staticAuthorization = null;
                }
            }
        }

        startLockExtension(context);
    }

    private void startLockExtension(final ConfigurationContext context) {
        maxLockLifetime = context.getProperty(MAX_LOCK_LIFETIME).asTimePeriod(TimeUnit.MILLISECONDS);
        try {
            int invalid = leases.load(getStateManager().getState(Scope.LOCAL).toMap());
            if (invalid > 0) {
                getLogger().warn("Skipped {} invalid external task lock entries in local state", invalid);
            }
        } catch (IOException e) {
            getLogger().warn("Failed to restore external task locks to extend from local state", e);
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "Operaton External Task Lock Extension " + getIdentifier());
            thread.setDaemon(true);
            return thread;
        });
        extending = true;
        executor.scheduleWithFixedDelay(this::extendLocks, lockExtensionInterval,
                lockExtensionInterval, TimeUnit.MILLISECONDS);
        lockExtender = executor;
    }

    private void stopLockExtension() {
        extending = false;
        ScheduledExecutorService executor = lockExtender;
        lockExtender = null;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            persistLeases();
        }
        leases.clear();
    }

    @OnDisabled
    public void onDisabled() {
        stopLockExtension();
        HttpClient httpClient = client;
        client = null;
        if (httpClient != null) {
            // close() would wait for in-flight (long polling) requests to finish
            httpClient.shutdownNow();
        }
        if (tokenManager != null) {
            tokenManager.invalidate();
            tokenManager = null;
        }
        oauth2Provider = null;
        staticAuthorization = null;
    }

    OAuth2TokenManager getTokenManager() {
        return tokenManager;
    }

    void setTokenManager(OAuth2TokenManager tokenManager) {
        this.tokenManager = tokenManager;
    }

    void setClock(LongSupplier clock) {
        this.clock = clock;
    }

    ExternalTaskLeases getLeases() {
        return leases;
    }

    void setLockExtensionInterval(long lockExtensionInterval) {
        this.lockExtensionInterval = lockExtensionInterval;
    }

    private void extendLocks() {
        try {
            extendDueLocks(clock.getAsLong());
        } catch (RuntimeException e) {
            // An exception would cancel the scheduled task
            getLogger().error("Failed to extend external task locks", e);
        }
    }

    /**
     * Extend the locks that are half way through their duration, up to the Max Lock Lifetime,
     * the soonest expiring first. Synchronized so that rounds do not overlap.
     */
    synchronized void extendDueLocks(long now) {
        for (Map.Entry<String, ExternalTaskLeases.Lease> entry : leases.due(now)) {
            if (!extending) {
                break;
            }
            String taskId = entry.getKey();
            // Re-read, as the task may have been finished or fetched again meanwhile
            ExternalTaskLeases.Lease lease = leases.get(taskId);
            if (lease == null || lease.renewAt() > now) {
                continue;
            }
            long newExpiresAt = Math.min(now + lease.lockDuration(), lease.lockedAt() + maxLockLifetime);
            if (newExpiresAt <= lease.expiresAt()) {
                if (leases.remove(taskId, lease)) {
                    getLogger().warn("Stopped extending the lock of external task {} after Max Lock "
                            + "Lifetime; it expires at {}. Was its FlowFile dropped without finishing "
                            + "the task?", taskId, Instant.ofEpochMilli(lease.expiresAt()));
                }
                continue;
            }
            // Bound the request so that one slow call does not delay the other extensions
            Duration timeout = Duration.ofMillis(Math.min(requestTimeout.toMillis(),
                    Math.max(lease.lockDuration() / 4, LOCK_EXTENSION_INTERVAL_MILLIS)));
            try {
                postExtendLock(taskId, newExpiresAt - now, timeout);
                leases.renew(taskId, lease, newExpiresAt);
            } catch (OperatonException e) {
                int status = e.getStatusCode();
                if (status == 400 || status == 404) {
                    // The task is gone, or its lock expired or was taken by another worker
                    if (leases.remove(taskId, lease)) {
                        getLogger().warn("Stopped extending the lock of external task {}: {}",
                                taskId, e.getMessage());
                    }
                } else {
                    getLogger().warn("Failed to extend the lock of external task {}, will retry: {}",
                            taskId, e.getMessage());
                    if (status == 0 || status == 502 || status == 503 || status == 504) {
                        // Operaton is unavailable; retry all due locks on the next round
                        break;
                    }
                }
            }
        }
        persistLeases();
    }

    private synchronized void persistLeases() {
        if (!leases.takeDirty()) {
            return;
        }
        try {
            getStateManager().setState(leases.toState(), Scope.LOCAL);
        } catch (IOException e) {
            leases.markDirty();
            getLogger().warn("Failed to store external task locks to extend in local state", e);
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
        // Not after the service has been disabled, as the leases would not be persisted
        // Locks that already last as long as Max Lock Lifetime would never be extended
        if (extending && !tasks.isEmpty() && query.lockDuration() < maxLockLifetime) {
            if (query.lockDuration() < MIN_EXTENDED_LOCK_DURATION_MILLIS) {
                getLogger().warn("Not extending locks of {} tasks of topic {} automatically: Lock Duration "
                        + "{} ms is shorter than {} ms", tasks.size(), query.topicName(),
                        query.lockDuration(), MIN_EXTENDED_LOCK_DURATION_MILLIS);
            } else {
                long now = clock.getAsLong();
                tasks.forEach(task -> leases.register(task.id(), query.lockDuration(), now));
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
        leases.release(taskId);
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
        leases.release(taskId);
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
        leases.release(taskId);
    }

    private void postExtendLock(String taskId, long newDuration, Duration timeout) throws OperatonException {
        ObjectNode body = mapper.createObjectNode()
                .put("workerId", workerId)
                .put("newDuration", newDuration);
        post(taskPath(taskId, "extendLock"), body, timeout);
    }

    @Override
    public void unlock(String taskId) throws OperatonException {
        post(taskPath(taskId, "unlock"), mapper.createObjectNode(), requestTimeout);
        leases.release(taskId);
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

    private String getAuthorizationHeader() throws OperatonException {
        OAuth2AccessTokenProvider provider = this.oauth2Provider;
        if (provider != null) {
            AccessToken details = provider.getAccessDetails();
            if (details == null || details.getAccessToken() == null || details.getAccessToken().isBlank()) {
                throw new OperatonException("OAuth2 Access Token Provider returned an empty access token", 0);
            }
            return "Bearer " + details.getAccessToken().trim();
        }
        OAuth2TokenManager tm = this.tokenManager;
        if (tm != null) {
            return "Bearer " + tm.getAccessToken();
        }
        return staticAuthorization;
    }

    /**
     * POST a JSON body and return the parsed response, or null for an empty response.
     */
    private JsonNode post(String path, JsonNode body, Duration timeout) throws OperatonException {
        return post(path, body, timeout, true);
    }

    private JsonNode post(String path, JsonNode body, Duration timeout, boolean retryOnUnauthorized)
            throws OperatonException {
        HttpClient httpClient = client;
        if (httpClient == null) {
            throw new OperatonException("Controller service is not enabled", 0);
        }

        String authHeader = getAuthorizationHeader();

        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
        if (authHeader != null) {
            request.header("Authorization", authHeader);
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
        if (status == 401 && retryOnUnauthorized) {
            OAuth2AccessTokenProvider provider = this.oauth2Provider;
            OAuth2TokenManager tm = this.tokenManager;
            if (provider != null) {
                provider.refreshAccessDetails();
                return post(path, body, timeout, false);
            } else if (tm != null) {
                String usedToken = authHeader != null && authHeader.startsWith("Bearer ")
                        ? authHeader.substring(7).trim() : null;
                tm.invalidate(usedToken);
                return post(path, body, timeout, false);
            }
        }

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
            extPropsNode.properties().forEach(entry -> {
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
