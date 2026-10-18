package org.operaton.bpm.extension.nifi.processor;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.operaton.bpm.extension.nifi.controller.OperatonException;
import org.operaton.bpm.extension.nifi.controller.OperatonExternalTaskService;

/**
 * Base class for processors acting on an external task locked by {@link FetchAndLockExternalTasks}.
 */
public abstract class AbstractExternalTaskProcessor extends AbstractProcessor {

    public static final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
            .name("External Task Service")
            .description("Operaton External Task client service")
            .identifiesControllerService(OperatonExternalTaskService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TASK_ID = new PropertyDescriptor.Builder()
            .name("External Task ID")
            .description("Id of the locked external task")
            .required(true)
            .defaultValue("${" + ExternalTaskAttributes.ID + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles whose external task was handled successfully")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles whose request failed transiently (no response, HTTP 429 or 5xx) "
                    + "and may succeed if retried; see the " + ExternalTaskAttributes.ERROR_MESSAGE + " attribute")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be handled; see the "
                    + ExternalTaskAttributes.ERROR_MESSAGE + " attribute")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_RETRY, REL_FAILURE);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        OperatonExternalTaskService service =
                context.getProperty(SERVICE).asControllerService(OperatonExternalTaskService.class);
        String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();

        int status = 0;
        String message;
        boolean transientError = false;
        Exception failureException;
        try {
            if (taskId == null || taskId.isBlank()) {
                throw new IllegalArgumentException("External task id is empty");
            }
            handle(context, session, flowFile, service, taskId);
            session.getProvenanceReporter().send(flowFile, "operaton:external-task/" + taskId);
            session.transfer(flowFile, REL_SUCCESS);
            return;
        } catch (OperatonException e) {
            failureException = e;
            status = e.getStatusCode();
            message = e.getMessage();
            if (status == 0 || status == 429 || status >= 500) {
                transientError = true;
            }
        } catch (Exception e) {
            failureException = e;
            message = e.getMessage() == null ? e.toString() : e.getMessage();
        }

        getLogger().error("Failed to handle external task {} for {}: {}", taskId, flowFile, message, failureException);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(ExternalTaskAttributes.ERROR_STATUS, String.valueOf(status));
        attributes.put(ExternalTaskAttributes.ERROR_MESSAGE, message);
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(session.penalize(flowFile), transientError ? REL_RETRY : REL_FAILURE);
    }

    /**
     * Act on the external task. Throwing transfers the FlowFile to retry or failure.
     */
    protected abstract void handle(ProcessContext context, ProcessSession session, FlowFile flowFile,
                                   OperatonExternalTaskService service, String taskId)
            throws Exception;

    /**
     * Read FlowFile content as typed variables JSON in the given format. Original types of
     * fetched variables are restored from the {@link ExternalTaskAttributes#VARIABLE_TYPES} attribute.
     */
    protected static String readVariables(ProcessSession session, FlowFile flowFile,
                                          VariablesFormat format) throws Exception {
        if (format == VariablesFormat.NONE) {
            return null;
        }
        byte[] content;
        try (InputStream in = session.read(flowFile)) {
            content = in.readAllBytes();
        }
        return Variables.toTyped(content, format,
                flowFile.getAttribute(ExternalTaskAttributes.VARIABLE_TYPES));
    }

    protected static List<PropertyDescriptor> properties(PropertyDescriptor... extra) {
        return Stream.concat(Stream.of(SERVICE, TASK_ID), Arrays.stream(extra)).toList();
    }
}
