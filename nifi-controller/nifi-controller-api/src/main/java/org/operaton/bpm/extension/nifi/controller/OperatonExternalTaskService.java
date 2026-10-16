package org.operaton.bpm.extension.nifi.controller;

import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * Client for the Operaton External Task REST API.
 *
 * <p>Process variables cross this API as JSON strings in Operaton's typed
 * variable format, e.g. {@code {"name": {"type": "String", "value": "x"}}},
 * so that implementations and callers do not need to share a JSON library.
 */
@Tags({"operaton", "bpmn", "external task", "workflow"})
@CapabilityDescription("Client for the Operaton External Task REST API.")
public interface OperatonExternalTaskService extends ControllerService {

    /**
     * @return the worker id used to lock, complete and fail external tasks
     */
    String getWorkerId();

    /**
     * Extended query parameters for {@link #fetchAndLock(FetchAndLockQuery)} matching
     * Operaton's {@code POST /external-task/fetchAndLock} payload.
     */
    record FetchAndLockQuery(
            String topicName,
            int maxTasks,
            long lockDuration,
            List<String> variables,
            long asyncResponseTimeout,
            boolean usePriority,
            boolean localVariables,
            boolean deserializeValues,
            boolean includeExtensionProperties,
            String businessKey,
            String processDefinitionId,
            List<String> processDefinitionIdIn,
            String processDefinitionKey,
            List<String> processDefinitionKeyIn,
            String processDefinitionVersionTag,
            boolean withoutTenantId,
            List<String> tenantIdIn,
            String processVariablesJson) {

        public FetchAndLockQuery(String topicName, int maxTasks, long lockDuration,
                                 List<String> variables, long asyncResponseTimeout) {
            this(topicName, maxTasks, lockDuration, variables, asyncResponseTimeout,
                    true, false, true, false, null, null, null, null, null, null, false, null, null);
        }
    }

    /**
     * Fetch and lock external tasks of a single topic.
     *
     * @param topicName topic to fetch tasks for
     * @param maxTasks maximum number of tasks to lock
     * @param lockDuration lock duration in milliseconds
     * @param variables names of variables to fetch, or {@code null} for all
     * @param asyncResponseTimeout long polling timeout in milliseconds, 0 to disable
     * @return locked tasks, possibly empty
     */
    List<ExternalTask> fetchAndLock(String topicName, int maxTasks, long lockDuration,
                                    List<String> variables, long asyncResponseTimeout)
            throws OperatonException;

    /**
     * Fetch and lock external tasks using extended topic filters and options.
     *
     * @param query fetch and lock query parameters
     * @return locked tasks, possibly empty
     */
    default List<ExternalTask> fetchAndLock(FetchAndLockQuery query) throws OperatonException {
        return fetchAndLock(query.topicName(), query.maxTasks(), query.lockDuration(),
                query.variables(), query.asyncResponseTimeout());
    }

    /**
     * Complete a locked external task.
     *
     * @param variablesJson typed process variables to set, or {@code null}
     * @param localVariablesJson typed local variables to set, or {@code null}
     */
    void complete(String taskId, String variablesJson, String localVariablesJson)
            throws OperatonException;

    /**
     * Report a failure. When {@code retries} reaches 0 an incident is created.
     *
     * @param retryTimeout milliseconds until the task can be fetched again
     */
    void handleFailure(String taskId, String errorMessage, String errorDetails,
                       int retries, long retryTimeout) throws OperatonException;

    /**
     * Report a failure and optionally set process or local variables on the task's execution.
     *
     * @param retryTimeout milliseconds until the task can be fetched again
     * @param variablesJson typed process variables to set, or {@code null}
     * @param localVariablesJson typed local variables to set, or {@code null}
     */
    default void handleFailure(String taskId, String errorMessage, String errorDetails,
                               int retries, long retryTimeout,
                               String variablesJson, String localVariablesJson)
            throws OperatonException {
        handleFailure(taskId, errorMessage, errorDetails, retries, retryTimeout);
    }

    /**
     * Throw a BPMN error to be caught by a boundary or event sub process.
     *
     * @param variablesJson typed process variables to set, or {@code null}
     */
    void handleBpmnError(String taskId, String errorCode, String errorMessage,
                         String variablesJson) throws OperatonException;

    /**
     * Extend the lock of a locked external task.
     *
     * @param newDuration new lock duration in milliseconds from now
     */
    void extendLock(String taskId, long newDuration) throws OperatonException;

    /**
     * Unlock a locked external task so that it can be fetched again immediately.
     */
    void unlock(String taskId) throws OperatonException;
}
