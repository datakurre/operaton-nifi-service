package org.operaton.bpm.extension.nifi.processor;

import java.util.List;

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

@Tags({"operaton", "bpmn", "external task", "workflow", "error"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Throws a BPMN error for a locked Operaton external task, to be caught by "
        + "an error boundary event or event sub process in the BPMN model.")
@SeeAlso({FetchAndLockExternalTasks.class})
@ReadsAttribute(attribute = ExternalTaskAttributes.ID, description = "External task id (default)")
@WritesAttributes({
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_STATUS,
                description = "HTTP status of a failed request, 0 if none was received"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_MESSAGE,
                description = "Error message of a failed request")
})
public class HandleExternalTaskBpmnError extends AbstractExternalTaskProcessor {

    public static final PropertyDescriptor ERROR_CODE = new PropertyDescriptor.Builder()
            .name("Error Code")
            .description("BPMN error code matched against error events in the BPMN model")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ERROR_MESSAGE = new PropertyDescriptor.Builder()
            .name("Error Message")
            .description("BPMN error message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor VARIABLES_FORMAT = new PropertyDescriptor.Builder()
            .name("Variables Format")
            .description("Format of the variables in FlowFile content")
            .required(true)
            .allowableValues(VariablesFormat.class)
            .defaultValue(VariablesFormat.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            properties(ERROR_CODE, ERROR_MESSAGE, VARIABLES_FORMAT);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void handle(ProcessContext context, ProcessSession session, FlowFile flowFile,
                          OperatonExternalTaskService service, String taskId) throws Exception {
        service.handleBpmnError(taskId,
                context.getProperty(ERROR_CODE).evaluateAttributeExpressions(flowFile).getValue(),
                context.getProperty(ERROR_MESSAGE).evaluateAttributeExpressions(flowFile).getValue(),
                readVariables(session, flowFile,
                        context.getProperty(VARIABLES_FORMAT).asAllowableValue(VariablesFormat.class)));
    }
}
