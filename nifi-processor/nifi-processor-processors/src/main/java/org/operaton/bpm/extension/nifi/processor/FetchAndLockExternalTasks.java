package org.operaton.bpm.extension.nifi.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.operaton.bpm.extension.nifi.controller.ExternalTask;
import org.operaton.bpm.extension.nifi.controller.OperatonException;
import org.operaton.bpm.extension.nifi.controller.OperatonExternalTaskService;

@Tags({"operaton", "bpmn", "external task", "workflow", "fetch", "lock"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Fetches and locks Operaton external tasks of a topic and emits one FlowFile "
        + "per task with the fetched process variables as JSON content. Finish each task with "
        + "CompleteExternalTask, HandleExternalTaskFailure or HandleExternalTaskBpmnError before "
        + "its lock expires, or the task will be fetched again. The locks are extended automatically "
        + "by the External Task Service until the task is finished.")
@SeeAlso({CompleteExternalTask.class, HandleExternalTaskFailure.class,
        HandleExternalTaskBpmnError.class, UnlockExternalTask.class})
@WritesAttributes({
        @WritesAttribute(attribute = ExternalTaskAttributes.ID, description = "External task id"),
        @WritesAttribute(attribute = ExternalTaskAttributes.TOPIC_NAME, description = "Topic name"),
        @WritesAttribute(attribute = ExternalTaskAttributes.WORKER_ID, description = "Worker id holding the lock"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ACTIVITY_ID, description = "BPMN activity id"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ACTIVITY_INSTANCE_ID, description = "Activity instance id"),
        @WritesAttribute(attribute = ExternalTaskAttributes.EXECUTION_ID, description = "Execution id"),
        @WritesAttribute(attribute = ExternalTaskAttributes.PROCESS_INSTANCE_ID, description = "Process instance id"),
        @WritesAttribute(attribute = ExternalTaskAttributes.PROCESS_DEFINITION_ID, description = "Process definition id"),
        @WritesAttribute(attribute = ExternalTaskAttributes.PROCESS_DEFINITION_KEY, description = "Process definition key"),
        @WritesAttribute(attribute = ExternalTaskAttributes.PROCESS_DEFINITION_VERSION_TAG, description = "Process definition version tag, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.BUSINESS_KEY, description = "Business key, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.TENANT_ID, description = "Tenant id, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.RETRIES, description = "Remaining retries, if a failure was reported before"),
        @WritesAttribute(attribute = ExternalTaskAttributes.PRIORITY, description = "Task priority"),
        @WritesAttribute(attribute = ExternalTaskAttributes.CREATE_TIME, description = "Task creation timestamp"),
        @WritesAttribute(attribute = ExternalTaskAttributes.LOCK_EXPIRATION_TIME, description = "Lock expiration time"),
        @WritesAttribute(attribute = ExternalTaskAttributes.TASK_ERROR_MESSAGE, description = "Last reported error message, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.TASK_ERROR_DETAILS, description = "Last reported error details, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.VARIABLE_TYPES, description = "Original Operaton types "
                + "of the fetched variables (Simple JSON format only), used to restore them on completion"),
        @WritesAttribute(attribute = "operaton.externalTask.extensionProperty.<name>",
                description = "BPMN extension properties when Include Extension Properties is true"),
        @WritesAttribute(attribute = "mime.type", description = "application/json")
})
public class FetchAndLockExternalTasks extends AbstractProcessor
        implements VerifiableProcessor {

    public static final PropertyDescriptor SERVICE = AbstractExternalTaskProcessor.SERVICE;

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("External task topic to fetch tasks for")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_TASKS = new PropertyDescriptor.Builder()
            .name("Max Tasks")
            .description("Maximum number of tasks to lock per fetch")
            .required(true)
            .defaultValue("10")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOCK_DURATION = new PropertyDescriptor.Builder()
            .name("Lock Duration")
            .description("Initial lock duration of fetched tasks. The External Task Service extends the locks "
                    + "in the background until the tasks are finished or Max Lock Lifetime has passed.")
            .required(true)
            .defaultValue("5 min")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor VARIABLES = new PropertyDescriptor.Builder()
            .name("Variables")
            .description("Comma separated names of variables to fetch. All variables are fetched "
                    + "when not set.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor LONG_POLLING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Long Polling Timeout")
            .description("How long Operaton may hold the request open waiting for tasks. "
                    + "The processor's thread is blocked meanwhile. 0 disables long polling.")
            .required(true)
            .defaultValue("0 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor VARIABLES_FORMAT = new PropertyDescriptor.Builder()
            .name("Variables Format")
            .description("Format of the variables written as FlowFile content")
            .required(true)
            .allowableValues(VariablesFormat.SIMPLE, VariablesFormat.TYPED)
            .defaultValue(VariablesFormat.SIMPLE)
            .build();

    public static final PropertyDescriptor USE_PRIORITY = new PropertyDescriptor.Builder()
            .name("Use Priority")
            .description("Whether tasks should be fetched ordered by arbitrary priority")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor LOCAL_VARIABLES = new PropertyDescriptor.Builder()
            .name("Local Variables")
            .description("When true, only variables local to the external task's execution are fetched")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor DESERIALIZE_VALUES = new PropertyDescriptor.Builder()
            .name("Deserialize Values")
            .description("Whether serializable variable values (e.g.Spin JSON/XML or Java objects) "
                    + "are deserialized on the server side by Operaton")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor INCLUDE_EXTENSION_PROPERTIES = new PropertyDescriptor.Builder()
            .name("Include Extension Properties")
            .description("Whether to include custom BPMN extension properties defined on the external "
                    + "service task as operaton.externalTask.extensionProperty.<name> attributes")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor BUSINESS_KEY = new PropertyDescriptor.Builder()
            .name("Business Key")
            .description("Filter tasks by process instance business key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROCESS_DEFINITION_ID = new PropertyDescriptor.Builder()
            .name("Process Definition ID")
            .description("Filter tasks by process definition id")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROCESS_DEFINITION_KEY = new PropertyDescriptor.Builder()
            .name("Process Definition Key")
            .description("Filter tasks by process definition key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROCESS_DEFINITION_VERSION_TAG = new PropertyDescriptor.Builder()
            .name("Process Definition Version Tag")
            .description("Filter tasks by process definition version tag")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor WITHOUT_TENANT_ID = new PropertyDescriptor.Builder()
            .name("Without Tenant ID")
            .description("When true, only fetch tasks that belong to no tenant")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor TENANT_IDS = new PropertyDescriptor.Builder()
            .name("Tenant IDs")
            .description("Comma separated list of tenant ids to filter tasks by")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("One FlowFile per locked external task")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            SERVICE, TOPIC_NAME, MAX_TASKS, LOCK_DURATION, VARIABLES, LONG_POLLING_TIMEOUT,
            VARIABLES_FORMAT, USE_PRIORITY, LOCAL_VARIABLES, DESERIALIZE_VALUES,
            INCLUDE_EXTENSION_PROPERTIES, BUSINESS_KEY, PROCESS_DEFINITION_ID,
            PROCESS_DEFINITION_KEY, PROCESS_DEFINITION_VERSION_TAG, WITHOUT_TENANT_ID, TENANT_IDS);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile OperatonExternalTaskService.FetchAndLockQuery query;
    private volatile boolean hasExtendedOptions;
    private volatile VariablesFormat variablesFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
        int maxTasks = context.getProperty(MAX_TASKS).evaluateAttributeExpressions().asInteger();
        long lockDuration = context.getProperty(LOCK_DURATION).evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.MILLISECONDS);
        List<String> variableNames = csvList(
                context.getProperty(VARIABLES).evaluateAttributeExpressions().getValue());
        long asyncResponseTimeout = context.getProperty(LONG_POLLING_TIMEOUT)
                .evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        this.variablesFormat = context.getProperty(VARIABLES_FORMAT).asAllowableValue(VariablesFormat.class);

        boolean usePriority = context.getProperty(USE_PRIORITY).asBoolean();
        boolean localVariables = context.getProperty(LOCAL_VARIABLES).asBoolean();
        boolean deserializeValues = context.getProperty(DESERIALIZE_VALUES).asBoolean();
        boolean includeExtensionProperties = context.getProperty(INCLUDE_EXTENSION_PROPERTIES).asBoolean();
        String businessKey = context.getProperty(BUSINESS_KEY).evaluateAttributeExpressions().getValue();
        String processDefinitionId = context.getProperty(PROCESS_DEFINITION_ID)
                .evaluateAttributeExpressions().getValue();
        String processDefinitionKey = context.getProperty(PROCESS_DEFINITION_KEY)
                .evaluateAttributeExpressions().getValue();
        String processDefinitionVersionTag = context.getProperty(PROCESS_DEFINITION_VERSION_TAG)
                .evaluateAttributeExpressions().getValue();
        boolean withoutTenantId = context.getProperty(WITHOUT_TENANT_ID).asBoolean();
        List<String> tenantIds = csvList(
                context.getProperty(TENANT_IDS).evaluateAttributeExpressions().getValue());

        this.hasExtendedOptions = !usePriority || localVariables || !deserializeValues
                || includeExtensionProperties || businessKey != null
                || processDefinitionId != null || processDefinitionKey != null
                || processDefinitionVersionTag != null || withoutTenantId || tenantIds != null;

        this.query = new OperatonExternalTaskService.FetchAndLockQuery(
                topicName, maxTasks, lockDuration, variableNames, asyncResponseTimeout,
                usePriority, localVariables, deserializeValues, includeExtensionProperties,
                businessKey, processDefinitionId, null, processDefinitionKey, null,
                processDefinitionVersionTag, withoutTenantId, tenantIds, null);
    }

    @OnStopped
    public void onStopped() {
        this.query = null;
        this.hasExtendedOptions = false;
        this.variablesFormat = null;
    }

    OperatonExternalTaskService.FetchAndLockQuery getQuery() {
        return query;
    }

    boolean isHasExtendedOptions() {
        return hasExtendedOptions;
    }

    VariablesFormat getVariablesFormat() {
        return variablesFormat;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        OperatonExternalTaskService service =
                context.getProperty(SERVICE).asControllerService(OperatonExternalTaskService.class);
        OperatonExternalTaskService.FetchAndLockQuery query = this.query;
        VariablesFormat format = this.variablesFormat;
        boolean hasExtendedOptions = this.hasExtendedOptions;
        if (query == null) {
            onScheduled(context);
            query = this.query;
            format = this.variablesFormat;
            hasExtendedOptions = this.hasExtendedOptions;
        }

        List<ExternalTask> tasks;
        try {
            if (hasExtendedOptions) {
                tasks = service.fetchAndLock(query);
            } else {
                tasks = service.fetchAndLock(
                        query.topicName(), query.maxTasks(), query.lockDuration(),
                        query.variables(), query.asyncResponseTimeout());
            }
        } catch (OperatonException e) {
            getLogger().error("Failed to fetch and lock external tasks of topic {}", query.topicName(), e);
            context.yield();
            return;
        }

        if (tasks.isEmpty()) {
            context.yield();
            return;
        }

        try {
            for (ExternalTask task : tasks) {
                Map<String, String> attributes = attributes(task);
                byte[] content = Variables.fromTyped(task.variablesJson(), format);
                if (format == VariablesFormat.SIMPLE) {
                    attributes.put(ExternalTaskAttributes.VARIABLE_TYPES, Variables.types(task.variablesJson()));
                }
                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, out -> out.write(content));
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().receive(flowFile,
                        service.getTransitUri(task.id()));
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            // The session is rolled back, so release the locks instead of leaving the tasks
            // locked without FlowFiles until the locks expire.
            unlock(service, tasks);
            throw new ProcessException("Failed to create FlowFiles for locked external tasks", e);
        }

        session.commitAsync(
                () -> { /* nothing */ },
                failure -> {
                    getLogger().error("Failed to commit FlowFiles for {} locked external tasks; unlocking them",
                            tasks.size(), failure);
                    unlock(service, tasks);
                });
    }

    private void unlock(OperatonExternalTaskService service, List<ExternalTask> tasks) {
        for (ExternalTask task : tasks) {
            try {
                service.unlock(task.id());
            } catch (OperatonException e) {
                getLogger().warn("Failed to unlock external task {}", task.id(), e);
            }
        }
    }

    /**
     * @return trimmed comma-separated values, or null when empty/unset
     */
    private static List<String> csvList(String value) {
        if (value == null) {
            return null;
        }
        List<String> items = Arrays.stream(value.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
        return items.isEmpty() ? null : items;
    }

    private static Map<String, String> attributes(ExternalTask task) {
        Map<String, String> attributes = new HashMap<>();
        put(attributes, ExternalTaskAttributes.ID, task.id());
        put(attributes, ExternalTaskAttributes.TOPIC_NAME, task.topicName());
        put(attributes, ExternalTaskAttributes.WORKER_ID, task.workerId());
        put(attributes, ExternalTaskAttributes.ACTIVITY_ID, task.activityId());
        put(attributes, ExternalTaskAttributes.ACTIVITY_INSTANCE_ID, task.activityInstanceId());
        put(attributes, ExternalTaskAttributes.EXECUTION_ID, task.executionId());
        put(attributes, ExternalTaskAttributes.PROCESS_INSTANCE_ID, task.processInstanceId());
        put(attributes, ExternalTaskAttributes.PROCESS_DEFINITION_ID, task.processDefinitionId());
        put(attributes, ExternalTaskAttributes.PROCESS_DEFINITION_KEY, task.processDefinitionKey());
        put(attributes, ExternalTaskAttributes.PROCESS_DEFINITION_VERSION_TAG, task.processDefinitionVersionTag());
        put(attributes, ExternalTaskAttributes.BUSINESS_KEY, task.businessKey());
        put(attributes, ExternalTaskAttributes.TENANT_ID, task.tenantId());
        put(attributes, ExternalTaskAttributes.RETRIES, task.retries());
        put(attributes, ExternalTaskAttributes.PRIORITY, task.priority());
        put(attributes, ExternalTaskAttributes.CREATE_TIME, task.createTime());
        put(attributes, ExternalTaskAttributes.LOCK_EXPIRATION_TIME, task.lockExpirationTime());
        put(attributes, ExternalTaskAttributes.TASK_ERROR_MESSAGE, task.errorMessage());
        put(attributes, ExternalTaskAttributes.TASK_ERROR_DETAILS, task.errorDetails());
        if (task.extensionProperties() != null) {
            task.extensionProperties().forEach((k, v) ->
                    put(attributes, ExternalTaskAttributes.EXTENSION_PROPERTY_PREFIX + k, v));
        }
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        return attributes;
    }

    private static void put(Map<String, String> attributes, String key, Object value) {
        if (value != null) {
            attributes.put(key, value.toString());
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context,
                                                final ComponentLog verificationLogger,
                                                final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        // Step 1: Lock Duration
        Long lockDurationMs = (variables == null
                ? context.getProperty(LOCK_DURATION).evaluateAttributeExpressions()
                : context.getProperty(LOCK_DURATION).evaluateAttributeExpressions(variables))
                .asTimePeriod(TimeUnit.MILLISECONDS);
        if (lockDurationMs == null || lockDurationMs < 3000) {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Lock Duration")
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation("Lock Duration must be at least 3 seconds"
                            + (lockDurationMs == null ? "" : " (was " + lockDurationMs + " ms)")
                            + "; shorter locks cannot be automatically extended before expiring")
                    .build());
        } else {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Lock Duration")
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation("Lock Duration is valid (" + lockDurationMs + " ms)")
                    .build());
        }

        // Step 2: Pending Tasks
        OperatonExternalTaskService service = context.getProperty(SERVICE)
                .asControllerService(OperatonExternalTaskService.class);
        if (service == null) {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Pending Tasks")
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation("External Task Service is not configured")
                    .build());
        } else {
            String topicName = (variables == null
                    ? context.getProperty(TOPIC_NAME).evaluateAttributeExpressions()
                    : context.getProperty(TOPIC_NAME).evaluateAttributeExpressions(variables))
                    .getValue();
            if (topicName == null || topicName.isBlank()) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Pending Tasks")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation("Topic Name is not configured")
                        .build());
            } else {
                try {
                    long count = service.countTasks(topicName);
                    if (count > 0) {
                        results.add(new ConfigVerificationResult.Builder()
                                .verificationStepName("Pending Tasks")
                                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                                .explanation("Found " + count + " pending unlocked task(s) ready for topic '" + topicName + "'")
                                .build());
                    } else {
                        results.add(new ConfigVerificationResult.Builder()
                                .verificationStepName("Pending Tasks")
                                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                                .explanation("Found 0 pending tasks for topic '" + topicName + "'. Check that the topic name matches the BPMN process model if tasks were expected.")
                                .build());
                    }
                } catch (UnsupportedOperationException e) {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Pending Tasks")
                            .outcome(ConfigVerificationResult.Outcome.SKIPPED)
                            .explanation("Configured External Task Service does not support task counting")
                            .build());
                } catch (Exception e) {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Pending Tasks")
                            .outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation("Failed to query pending tasks for topic '" + topicName + "': " + e.getMessage())
                            .build());
                }
            }
        }

        return results;
    }
}
