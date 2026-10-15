package org.operaton.bpm.extension.nifi.controller;

/**
 * External task locked by {@link OperatonExternalTaskService#fetchAndLock}.
 *
 * @param variablesJson fetched variables as JSON in Operaton's typed variable format
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
        String businessKey,
        String tenantId,
        Integer retries,
        Long priority,
        String lockExpirationTime,
        String variablesJson) {
}
