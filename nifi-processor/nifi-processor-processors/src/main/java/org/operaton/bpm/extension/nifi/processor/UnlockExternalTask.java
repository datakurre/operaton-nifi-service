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

@Tags({"operaton", "bpmn", "external task", "workflow", "unlock"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Unlocks a locked Operaton external task so that it can be fetched again immediately "
        + "by any worker without waiting for its lock to expire.")
@SeeAlso({FetchAndLockExternalTasks.class, ExtendExternalTaskLock.class})
@ReadsAttribute(attribute = ExternalTaskAttributes.ID, description = "External task id (default)")
@WritesAttributes({
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_STATUS,
                description = "HTTP status of a failed request, 0 if none was received"),
        @WritesAttribute(attribute = ExternalTaskAttributes.ERROR_MESSAGE,
                description = "Error message of a failed request")
})
public class UnlockExternalTask extends AbstractExternalTaskProcessor {

    private static final List<PropertyDescriptor> PROPERTIES = properties();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void handle(ProcessContext context, ProcessSession session, FlowFile flowFile,
                          OperatonExternalTaskService service, String taskId) throws Exception {
        service.unlock(taskId);
    }
}
