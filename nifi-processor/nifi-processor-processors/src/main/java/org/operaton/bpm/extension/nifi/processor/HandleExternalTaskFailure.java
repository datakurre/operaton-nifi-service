package org.operaton.bpm.extension.nifi.processor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.operaton.bpm.extension.nifi.controller.OperatonExternalTaskService;

@Tags({"operaton", "bpmn", "external task", "workflow", "failure", "incident", "retry"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Reports a failure for a locked Operaton external task. The task is "
        + "retried after the retry timeout while retries remain; when retries reach 0, "
        + "Operaton creates an incident.")
@SeeAlso({FetchAndLockExternalTasks.class})
@ReadsAttributes({
        @ReadsAttribute(attribute = ExternalTaskAttributes.ID, description = "External task id (default)"),
        @ReadsAttribute(attribute = ExternalTaskAttributes.RETRIES,
                description = "Remaining retries; decremented when Retries is not set")
})
@WritesAttributes({
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_STATUS,
                description = "HTTP status of a failed request, 0 if none was received"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_MESSAGE,
                description = "Error message of a failed request")
})
public class HandleExternalTaskFailure extends AbstractExternalTaskProcessor {

    public static final PropertyDescriptor ERROR_MESSAGE = new PropertyDescriptor.Builder()
            .name("Error Message")
            .description("Short error message shown in Operaton (and in the incident)")
            .required(true)
            .defaultValue("${" + ExternalTaskAttributes.ERROR_MESSAGE
                    + ":replaceEmpty('Processing failed in Apache NiFi')}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ERROR_DETAILS = new PropertyDescriptor.Builder()
            .name("Error Details")
            .description("Detailed error description, e.g. a stack trace")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETRIES = new PropertyDescriptor.Builder()
            .name("Retries")
            .description("Remaining retries to set. When not set, the task's current retries "
                    + "(" + ExternalTaskAttributes.RETRIES + ") are decremented, or Default "
                    + "Retries is used on the first failure.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEFAULT_RETRIES = new PropertyDescriptor.Builder()
            .name("Default Retries")
            .description("Remaining retries to set on the first failure of a task")
            .required(true)
            .defaultValue("3")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETRY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Retry Timeout")
            .description("How long until the task can be fetched again")
            .required(true)
            .defaultValue("1 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            properties(ERROR_MESSAGE, ERROR_DETAILS, RETRIES, DEFAULT_RETRIES, RETRY_TIMEOUT);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void handle(ProcessContext context, ProcessSession session, FlowFile flowFile,
                          OperatonExternalTaskService service, String taskId) throws Exception {
        service.handleFailure(taskId,
                context.getProperty(ERROR_MESSAGE).evaluateAttributeExpressions(flowFile).getValue(),
                context.getProperty(ERROR_DETAILS).evaluateAttributeExpressions(flowFile).getValue(),
                retries(context, flowFile),
                context.getProperty(RETRY_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
    }

    private static int retries(ProcessContext context, FlowFile flowFile) {
        String retries = context.getProperty(RETRIES).evaluateAttributeExpressions(flowFile).getValue();
        if (retries != null && !retries.isBlank()) {
            return Integer.parseInt(retries.trim());
        }
        String current = flowFile.getAttribute(ExternalTaskAttributes.RETRIES);
        if (current != null && !current.isBlank()) {
            return Math.max(Integer.parseInt(current.trim()) - 1, 0);
        }
        return context.getProperty(DEFAULT_RETRIES).asInteger();
    }
}
