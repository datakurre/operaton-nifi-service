package org.operaton.bpm.extension.nifi.processor;

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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
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
        + "its lock expires, or the task will be fetched again.")
@SeeAlso({CompleteExternalTask.class, HandleExternalTaskFailure.class,
        HandleExternalTaskBpmnError.class, ExtendExternalTaskLock.class})
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
        @WritesAttribute(attribute = ExternalTaskAttributes.BUSINESS_KEY, description = "Business key, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.TENANT_ID, description = "Tenant id, if any"),
        @WritesAttribute(attribute = ExternalTaskAttributes.RETRIES, description = "Remaining retries, if a failure was reported before"),
        @WritesAttribute(attribute = ExternalTaskAttributes.PRIORITY, description = "Task priority"),
        @WritesAttribute(attribute = ExternalTaskAttributes.LOCK_EXPIRATION_TIME, description = "Lock expiration time"),
        @WritesAttribute(attribute = ExternalTaskAttributes.VARIABLE_TYPES, description = "Original Operaton types "
                + "of the fetched variables (Simple JSON format only), used to restore them on completion"),
        @WritesAttribute(attribute = "mime.type", description = "application/json")
})
public class FetchAndLockExternalTasks extends AbstractProcessor {

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
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOCK_DURATION = new PropertyDescriptor.Builder()
            .name("Lock Duration")
            .description("How long fetched tasks stay locked for this worker")
            .required(true)
            .defaultValue("5 min")
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
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor VARIABLES_FORMAT = new PropertyDescriptor.Builder()
            .name("Variables Format")
            .description("Format of the variables written as FlowFile content")
            .required(true)
            .allowableValues(VariablesFormat.SIMPLE, VariablesFormat.TYPED)
            .defaultValue(VariablesFormat.SIMPLE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("One FlowFile per locked external task")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            SERVICE, TOPIC_NAME, MAX_TASKS, LOCK_DURATION, VARIABLES, LONG_POLLING_TIMEOUT,
            VARIABLES_FORMAT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        OperatonExternalTaskService service =
                context.getProperty(SERVICE).asControllerService(OperatonExternalTaskService.class);
        String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
        String variables = context.getProperty(VARIABLES).evaluateAttributeExpressions().getValue();
        VariablesFormat format = context.getProperty(VARIABLES_FORMAT).asAllowableValue(VariablesFormat.class);

        List<ExternalTask> tasks;
        try {
            tasks = service.fetchAndLock(
                    topicName,
                    context.getProperty(MAX_TASKS).asInteger(),
                    context.getProperty(LOCK_DURATION).asTimePeriod(TimeUnit.MILLISECONDS),
                    variableNames(variables),
                    context.getProperty(LONG_POLLING_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
        } catch (OperatonException e) {
            getLogger().error("Failed to fetch and lock external tasks of topic {}", topicName, e);
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
                        "operaton:external-task/" + task.id());
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            // The session is rolled back, so release the locks instead of leaving the tasks
            // locked without FlowFiles until the locks expire.
            unlock(service, tasks);
            throw new ProcessException("Failed to create FlowFiles for locked external tasks", e);
        }
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
     * @return trimmed variable names, or null to fetch all variables
     */
    private static List<String> variableNames(String variables) {
        if (variables == null) {
            return null;
        }
        List<String> names = Arrays.stream(variables.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
        return names.isEmpty() ? null : names;
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
        put(attributes, ExternalTaskAttributes.BUSINESS_KEY, task.businessKey());
        put(attributes, ExternalTaskAttributes.TENANT_ID, task.tenantId());
        put(attributes, ExternalTaskAttributes.RETRIES, task.retries());
        put(attributes, ExternalTaskAttributes.PRIORITY, task.priority());
        put(attributes, ExternalTaskAttributes.LOCK_EXPIRATION_TIME, task.lockExpirationTime());
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        return attributes;
    }

    private static void put(Map<String, String> attributes, String key, Object value) {
        if (value != null) {
            attributes.put(key, value.toString());
        }
    }
}
