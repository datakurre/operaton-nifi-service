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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.operaton.bpm.extension.nifi.controller.OperatonExternalTaskService;

@Tags({"operaton", "bpmn", "external task", "workflow", "complete"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Completes a locked Operaton external task, optionally setting variables "
        + "from the FlowFile content.")
@SeeAlso({FetchAndLockExternalTasks.class})
@ReadsAttribute(attribute = ExternalTaskAttributes.ID, description = "External task id (default)")
@WritesAttributes({
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_STATUS,
                description = "HTTP status of a failed or retried request, 0 if none was received"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_MESSAGE,
                description = "Error message of a failed or retried request")
})
public class CompleteExternalTask extends AbstractExternalTaskProcessor {

    public static final PropertyDescriptor VARIABLES_FORMAT = new PropertyDescriptor.Builder()
            .name("Variables Format")
            .description("Format of the variables in FlowFile content")
            .required(true)
            .allowableValues(VariablesFormat.class)
            .defaultValue(VariablesFormat.SIMPLE)
            .build();

    public static final PropertyDescriptor LOCAL_VARIABLES = new PropertyDescriptor.Builder()
            .name("Local Variables")
            .description("Set variables locally on the external task's execution "
                    + "instead of in the process instance scope")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            properties(VARIABLES_FORMAT, LOCAL_VARIABLES);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void handle(ProcessContext context, ProcessSession session, FlowFile flowFile,
                          OperatonExternalTaskService service, String taskId) throws Exception {
        String variables = readVariables(session, flowFile,
                context.getProperty(VARIABLES_FORMAT).asAllowableValue(VariablesFormat.class));
        if (context.getProperty(LOCAL_VARIABLES).asBoolean()) {
            service.complete(taskId, null, variables);
        } else {
            service.complete(taskId, variables, null);
        }
    }
}
