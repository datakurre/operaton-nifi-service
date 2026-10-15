package org.operaton.bpm.extension.nifi.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.nifi.controller.AbstractControllerService;
import org.operaton.bpm.extension.nifi.controller.ExternalTask;
import org.operaton.bpm.extension.nifi.controller.OperatonException;
import org.operaton.bpm.extension.nifi.controller.OperatonExternalTaskService;

/**
 * Records calls and returns canned tasks.
 */
public class MockExternalTaskService extends AbstractControllerService
        implements OperatonExternalTaskService {

    /** A recorded service call. */
    public record Call(String method, List<Object> args) {
    }

    final List<Call> calls = new ArrayList<>();
    final List<ExternalTask> tasks = new ArrayList<>();
    OperatonException failure;

    @Override
    public String getWorkerId() {
        return "worker-1";
    }

    private void record(String method, Object... args) throws OperatonException {
        calls.add(new Call(method, Arrays.asList(args)));
        if (failure != null) {
            throw failure;
        }
    }

    Call lastCall() {
        return calls.get(calls.size() - 1);
    }

    @Override
    public List<ExternalTask> fetchAndLock(String topicName, int maxTasks, long lockDuration,
                                           List<String> variables, long asyncResponseTimeout)
            throws OperatonException {
        record("fetchAndLock", topicName, maxTasks, lockDuration, variables, asyncResponseTimeout);
        List<ExternalTask> result = List.copyOf(tasks);
        tasks.clear();
        return result;
    }

    @Override
    public void complete(String taskId, String variablesJson, String localVariablesJson)
            throws OperatonException {
        record("complete", taskId, variablesJson, localVariablesJson);
    }

    @Override
    public void handleFailure(String taskId, String errorMessage, String errorDetails,
                              int retries, long retryTimeout) throws OperatonException {
        record("handleFailure", taskId, errorMessage, errorDetails, retries, retryTimeout);
    }

    @Override
    public void handleBpmnError(String taskId, String errorCode, String errorMessage,
                                String variablesJson) throws OperatonException {
        record("handleBpmnError", taskId, errorCode, errorMessage, variablesJson);
    }

    @Override
    public void extendLock(String taskId, long newDuration) throws OperatonException {
        record("extendLock", taskId, newDuration);
    }

    @Override
    public void unlock(String taskId) throws OperatonException {
        calls.add(new Call("unlock", List.of(taskId)));
    }
}
