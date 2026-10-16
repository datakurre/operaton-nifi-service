package org.operaton.bpm.extension.nifi.processor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
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

@Tags({"operaton", "bpmn", "external task", "workflow", "lock"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Extends the lock of a locked Operaton external task, e.g. before a "
        + "long running step of the flow.")
@SeeAlso({FetchAndLockExternalTasks.class, UnlockExternalTask.class})
@ReadsAttribute(attribute = ExternalTaskAttributes.ID, description = "External task id (default)")
@WritesAttributes({
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_STATUS,
                description = "HTTP status of a failed request, 0 if none was received"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_MESSAGE,
                description = "Error message of a failed request")
})
public class ExtendExternalTaskLock extends AbstractExternalTaskProcessor {

    public static final PropertyDescriptor LOCK_DURATION = new PropertyDescriptor.Builder()
            .name("Lock Duration")
            .description("New lock duration, counted from now")
            .required(true)
            .defaultValue("5 min")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = properties(LOCK_DURATION);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void handle(ProcessContext context, ProcessSession session, FlowFile flowFile,
                          OperatonExternalTaskService service, String taskId) throws Exception {
        service.extendLock(taskId, context.getProperty(LOCK_DURATION)
                .evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS));
    }
}
