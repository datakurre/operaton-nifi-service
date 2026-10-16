package org.operaton.bpm.extension.nifi.controller;

import java.util.Map;

/**
 * External task locked by {@link OperatonExternalTaskService#fetchAndLock}.
 *
 * @param variablesJson fetched variables as JSON in Operaton's typed variable format
 * @param extensionProperties BPMN extension properties, if requested
 */
public record ExternalTask(
        String id,
        String topicName,
        String workerId,
        String activityId,
        String activityInstanceId,
        String executionId,
        String processInstanceId,
        String processDefinitionId,
        String processDefinitionKey,
        String processDefinitionVersionTag,
        String businessKey,
        String tenantId,
        Integer retries,
        Long priority,
        String createTime,
        String lockExpirationTime,
        String errorMessage,
        String errorDetails,
        String variablesJson,
        Map<String, String> extensionProperties) {

    public ExternalTask {
        extensionProperties = extensionProperties == null ? Map.of() : Map.copyOf(extensionProperties);
    }

    public ExternalTask(
            String id,
            String topicName,
            String workerId,
            String activityId,
            String activityInstanceId,
            String executionId,
            String processInstanceId,
            String processDefinitionId,
            String processDefinitionKey,
            String businessKey,
            String tenantId,
            Integer retries,
            Long priority,
            String lockExpirationTime,
            String variablesJson) {
        this(
                id,
                topicName,
                workerId,
                activityId,
                activityInstanceId,
                executionId,
                processInstanceId,
                processDefinitionId,
                processDefinitionKey,
                null,
                businessKey,
                tenantId,
                retries,
                priority,
                null,
                lockExpirationTime,
                null,
                null,
                variablesJson,
                Map.of());
    }
}
