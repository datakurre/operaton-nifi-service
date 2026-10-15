package org.operaton.bpm.extension.nifi.controller;

import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class TestProcessor extends AbstractProcessor {

    static final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
            .name("External Task Service")
            .identifiesControllerService(OperatonExternalTaskService.class)
            .required(true)
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(SERVICE);
    }
}
