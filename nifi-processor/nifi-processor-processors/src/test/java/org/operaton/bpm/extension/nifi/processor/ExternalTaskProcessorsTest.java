package org.operaton.bpm.extension.nifi.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.operaton.bpm.extension.nifi.controller.ExternalTask;
import org.operaton.bpm.extension.nifi.controller.OperatonException;
import org.operaton.bpm.extension.nifi.controller.OperatonExternalTaskService;

public class ExternalTaskProcessorsTest {

    private static final String TYPED_VARIABLES = """
            {"name": {"type": "String", "value": "Ada", "valueInfo": {}},
             "count": {"type": "Integer", "value": 2, "valueInfo": {}},
             "order": {"type": "Json", "value": "{\\"id\\":1}", "valueInfo": {}},
             "nothing": {"type": "Null", "value": null, "valueInfo": {}}}
            """;

    private MockExternalTaskService service;

    private TestRunner runner(Class<? extends Processor> processor) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(processor);
        service = new MockExternalTaskService();
        runner.addControllerService("operaton", service);
        runner.enableControllerService(service);
        runner.setProperty(AbstractExternalTaskProcessor.SERVICE, "operaton");
        return runner;
    }

    private static ExternalTask task(String id, Integer retries) {
        return new ExternalTask(id, "topic", "worker-1", "Activity_1", "Activity_1:ai", "ex-1",
                "pi-1", "pd-1", "process", "bk-1", null, retries, 0L,
                "2026-10-15T12:00:00.000+0000", TYPED_VARIABLES);
    }

    private static JsonNode json(String content) throws Exception {
        return Variables.MAPPER.readTree(content);
    }

    @Test
    public void testFetchAndLockSimpleVariables() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        runner.setProperty(FetchAndLockExternalTasks.VARIABLES, "name, count,");
        service.tasks.add(task("task-1", null));
        service.tasks.add(task("task-2", 2));

        runner.run();

        assertEquals(List.of("topic", 10, 300000L, List.of("name", "count"), 0L),
                service.lastCall().args());
        runner.assertAllFlowFilesTransferred(FetchAndLockExternalTasks.REL_SUCCESS, 2);
        MockFlowFile first = runner.getFlowFilesForRelationship(FetchAndLockExternalTasks.REL_SUCCESS).get(0);
        first.assertAttributeEquals(ExternalTaskAttributes.ID, "task-1");
        first.assertAttributeEquals(ExternalTaskAttributes.PROCESS_INSTANCE_ID, "pi-1");
        first.assertAttributeEquals(ExternalTaskAttributes.BUSINESS_KEY, "bk-1");
        first.assertAttributeNotExists(ExternalTaskAttributes.RETRIES);
        first.assertAttributeNotExists(ExternalTaskAttributes.TENANT_ID);
        first.assertAttributeEquals("mime.type", "application/json");
        assertEquals(json("{\"name\": \"Ada\", \"count\": 2, \"order\": {\"id\": 1}, \"nothing\": null}"),
                json(first.getContent()));
        runner.getFlowFilesForRelationship(FetchAndLockExternalTasks.REL_SUCCESS).get(1)
                .assertAttributeEquals(ExternalTaskAttributes.RETRIES, "2");
        assertEquals("Integer", json(first.getAttribute(ExternalTaskAttributes.VARIABLE_TYPES))
                .at("/count/type").asText());
        assertEquals(0, service.calls.stream().filter(c -> "unlock".equals(c.method())).count());
    }

    @Test
    public void testFetchAndLockCommaOnlyVariablesFetchesAll() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        runner.setProperty(FetchAndLockExternalTasks.VARIABLES, " , ");

        runner.run();

        assertNull(service.lastCall().args().get(3));
    }

    @Test
    public void testOnScheduledPrecomputesQueryAndOnStoppedClears() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        runner.setProperty(FetchAndLockExternalTasks.MAX_TASKS, "5");
        runner.setProperty(FetchAndLockExternalTasks.LOCK_DURATION, "10 sec");
        runner.setProperty(FetchAndLockExternalTasks.VARIABLES_FORMAT, VariablesFormat.TYPED.getValue());
        runner.setProperty(FetchAndLockExternalTasks.BUSINESS_KEY, "bk-123");

        FetchAndLockExternalTasks processor = (FetchAndLockExternalTasks) runner.getProcessor();
        processor.onScheduled(runner.getProcessContext());

        OperatonExternalTaskService.FetchAndLockQuery query = processor.getQuery();
        assertNotNull(query);
        assertEquals("orders", query.topicName());
        assertEquals(5, query.maxTasks());
        assertEquals(10000L, query.lockDuration());
        assertEquals("bk-123", query.businessKey());
        assertEquals(VariablesFormat.TYPED, processor.getVariablesFormat());
        assertTrue(processor.isHasExtendedOptions());

        processor.onStopped();
        assertNull(processor.getQuery());
        assertNull(processor.getVariablesFormat());
        assertFalse(processor.isHasExtendedOptions());
    }

    @Test
    public void testFetchAndLockUnlocksTasksOnFailure() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        service.tasks.add(task("task-1", null));
        service.tasks.add(new ExternalTask("task-2", "topic", "worker-1", null, null, null, null,
                null, null, null, null, null, null, null, "not json"));

        assertThrows(AssertionError.class, () -> runner.run());

        runner.assertTransferCount(FetchAndLockExternalTasks.REL_SUCCESS, 0);
        assertEquals(List.of("fetchAndLock", "unlock", "unlock"),
                service.calls.stream().map(MockExternalTaskService.Call::method).toList());
        assertEquals(List.of("task-2"), service.lastCall().args());
    }

    @Test
    public void testFetchAndLockUnlocksTasksOnCommitFailure() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        service.tasks.add(task("task-1", null));
        service.tasks.add(task("task-2", null));

        FetchAndLockExternalTasks processor = (FetchAndLockExternalTasks) runner.getProcessor();
        ProcessContext context = runner.getProcessContext();
        SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong(0));
        MockProcessSession session = new MockProcessSession(sharedState, processor) {
            @Override
            public void commitAsync(Runnable onSuccess, Consumer<Throwable> onFailure) {
                onFailure.accept(new RuntimeException("Simulated commit failure"));
            }
        };

        processor.onTrigger(context, session);

        assertEquals(List.of("task-1", "task-2"),
                service.calls.stream()
                        .filter(c -> "unlock".equals(c.method()))
                        .map(c -> (String) c.args().get(0))
                        .toList());
    }

    @Test
    public void testFetchThenCompleteKeepsTypes() throws Exception {
        TestRunner fetch = runner(FetchAndLockExternalTasks.class);
        fetch.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        service.tasks.add(new ExternalTask("task-1", "topic", "worker-1", null, null, null, null,
                null, null, null, null, null, null, null,
                "{\"n\": {\"type\": \"Long\", \"value\": 5, \"valueInfo\": {}}}"));
        fetch.run();
        MockFlowFile fetched = fetch.getFlowFilesForRelationship(FetchAndLockExternalTasks.REL_SUCCESS).get(0);

        TestRunner complete = runner(CompleteExternalTask.class);
        complete.enqueue(fetched);
        complete.run();

        complete.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        assertEquals(json("{\"n\": {\"type\": \"Long\", \"value\": 5}}"),
                json((String) service.lastCall().args().get(1)));
    }

    @Test
    public void testFetchAndLockTypedVariables() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        runner.setProperty(FetchAndLockExternalTasks.VARIABLES_FORMAT, VariablesFormat.TYPED);
        service.tasks.add(task("task-1", null));

        runner.run();

        assertNull(service.lastCall().args().get(3));
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchAndLockExternalTasks.REL_SUCCESS).get(0);
        assertEquals(json(TYPED_VARIABLES), json(flowFile.getContent()));
    }

    @Test
    public void testFetchAndLockError() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        service.failure = new OperatonException("down", 0);

        runner.run();

        runner.assertTransferCount(FetchAndLockExternalTasks.REL_SUCCESS, 0);
    }

    @Test
    public void testCompleteWithSimpleVariables() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);

        runner.enqueue("{\"s\": \"x\", \"i\": 1, \"l\": 12345678901, \"d\": 1.5, \"b\": true, "
                + "\"n\": null, \"o\": {\"a\": [1]}}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        List<Object> args = service.lastCall().args();
        assertEquals("task-1", args.get(0));
        assertNull(args.get(2));
        assertEquals(json("""
                {"s": {"type": "String", "value": "x"},
                 "i": {"type": "Integer", "value": 1},
                 "l": {"type": "Long", "value": 12345678901},
                 "d": {"type": "Double", "value": 1.5},
                 "b": {"type": "Boolean", "value": true},
                 "n": {"type": "Null", "value": null},
                 "o": {"type": "Json", "value": "{\\"a\\":[1]}"}}
                """), json((String) args.get(1)));
    }

    @Test
    public void testCompleteLocalTypedVariables() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        runner.setProperty(CompleteExternalTask.VARIABLES_FORMAT, VariablesFormat.TYPED);
        runner.setProperty(CompleteExternalTask.LOCAL_VARIABLES, "true");

        runner.enqueue("{\"a\": {\"type\": \"Short\", \"value\": 1}}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        List<Object> args = service.lastCall().args();
        assertNull(args.get(1));
        assertEquals(json("{\"a\": {\"type\": \"Short\", \"value\": 1}}"), json((String) args.get(2)));
    }

    @Test
    public void testCompleteWithoutVariables() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        runner.setProperty(CompleteExternalTask.VARIABLES_FORMAT, VariablesFormat.NONE);

        runner.enqueue("not json", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        assertNull(service.lastCall().args().get(1));
    }

    @Test
    public void testCompleteInvalidContent() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);

        runner.enqueue("[1, 2]", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_FAILURE, 1);
        assertTrue(service.calls.isEmpty());
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExternalTaskProcessor.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_STATUS, "0");
        flowFile.assertAttributeExists(ExternalTaskAttributes.ERROR_MESSAGE);
    }

    @Test
    public void testCompleteMissingTaskId() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);

        runner.enqueue("{}");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_FAILURE, 1);
        assertTrue(service.calls.isEmpty());
    }

    @Test
    public void testCompleteServiceError() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        service.failure = new OperatonException("not found", 404);

        runner.enqueue("{}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExternalTaskProcessor.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_STATUS, "404");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_MESSAGE, "not found");
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testCompleteService503TransfersToRetry() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        service.failure = new OperatonException("service unavailable", 503);

        runner.enqueue("{}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_RETRY, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExternalTaskProcessor.REL_RETRY).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_STATUS, "503");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_MESSAGE, "service unavailable");
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testCompleteService429TransfersToRetry() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        service.failure = new OperatonException("too many requests", 429);

        runner.enqueue("{}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_RETRY, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExternalTaskProcessor.REL_RETRY).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_STATUS, "429");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_MESSAGE, "too many requests");
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testCompleteServiceNoResponseTransfersToRetry() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        service.failure = new OperatonException("Connection refused", 0);

        runner.enqueue("{}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_RETRY, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExternalTaskProcessor.REL_RETRY).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_STATUS, "0");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_MESSAGE, "Connection refused");
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testCompleteService400TransfersToFailure() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        service.failure = new OperatonException("bad request", 400);

        runner.enqueue("{}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExternalTaskProcessor.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_STATUS, "400");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.ERROR_MESSAGE, "bad request");
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testHandleFailureFirstFailure() throws Exception {
        TestRunner runner = runner(HandleExternalTaskFailure.class);

        runner.enqueue("", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        assertEquals("handleFailure", service.lastCall().method());
        assertEquals(Arrays.asList("task-1", "Processing failed in Apache NiFi", null, 3, 60000L),
                service.lastCall().args());
    }

    @Test
    public void testHandleFailureDecrementsRetries() throws Exception {
        TestRunner runner = runner(HandleExternalTaskFailure.class);
        runner.setProperty(HandleExternalTaskFailure.ERROR_DETAILS, "${details}");

        runner.enqueue("", Map.of(ExternalTaskAttributes.ID, "task-1",
                ExternalTaskAttributes.RETRIES, "1",
                ExternalTaskAttributes.ERROR_MESSAGE, "HTTP 503",
                "details", "upstream down"));
        runner.run();

        assertEquals(List.of("task-1", "HTTP 503", "upstream down", 0, 60000L), service.lastCall().args());
    }

    @Test
    public void testHandleFailureExplicitRetries() throws Exception {
        TestRunner runner = runner(HandleExternalTaskFailure.class);
        runner.setProperty(HandleExternalTaskFailure.RETRIES, "0");
        runner.setProperty(HandleExternalTaskFailure.RETRY_TIMEOUT, "5 sec");

        runner.enqueue("", Map.of(ExternalTaskAttributes.ID, "task-1", ExternalTaskAttributes.RETRIES, "5"));
        runner.run();

        assertEquals(0, service.lastCall().args().get(3));
        assertEquals(5000L, service.lastCall().args().get(4));
    }

    @Test
    public void testHandleBpmnError() throws Exception {
        TestRunner runner = runner(HandleExternalTaskBpmnError.class);
        runner.setProperty(HandleExternalTaskBpmnError.ERROR_CODE, "${code}");
        runner.setProperty(HandleExternalTaskBpmnError.VARIABLES_FORMAT, VariablesFormat.SIMPLE);

        runner.enqueue("{\"reason\": \"rejected\"}", Map.of(ExternalTaskAttributes.ID, "task-1", "code", "REJECTED"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        List<Object> args = service.lastCall().args();
        assertEquals("handleBpmnError", service.lastCall().method());
        assertEquals("REJECTED", args.get(1));
        assertNull(args.get(2));
        assertEquals(json("{\"reason\": {\"type\": \"String\", \"value\": \"rejected\"}}"),
                json((String) args.get(3)));
    }

    @Test
    public void testFetchAndLockWithExtendedFiltersAndAttributes() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        runner.setProperty(FetchAndLockExternalTasks.BUSINESS_KEY, "bk-99");
        runner.setProperty(FetchAndLockExternalTasks.PROCESS_DEFINITION_KEY, "order-proc");
        runner.setProperty(FetchAndLockExternalTasks.LOCAL_VARIABLES, "true");
        runner.setProperty(FetchAndLockExternalTasks.INCLUDE_EXTENSION_PROPERTIES, "true");
        runner.setProperty(FetchAndLockExternalTasks.TENANT_IDS, "t1, t2");

        service.tasks.add(new ExternalTask(
                "task-ext", "orders", "worker-1", "Act_1", "Act_1:1", "ex-1",
                "pi-1", "pd-1", "order-proc", "v1.2", "bk-99", "t1",
                2, 10L, "2026-10-16T08:00:00.000+0000", "2026-10-16T08:05:00.000+0000",
                "last err", "last stack", "{}", Map.of("targetQueue", "priority-high")));

        runner.run();

        assertEquals("fetchAndLockQuery", service.lastCall().method());
        runner.assertAllFlowFilesTransferred(FetchAndLockExternalTasks.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchAndLockExternalTasks.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ExternalTaskAttributes.PROCESS_DEFINITION_VERSION_TAG, "v1.2");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.CREATE_TIME, "2026-10-16T08:00:00.000+0000");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.TASK_ERROR_MESSAGE, "last err");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.TASK_ERROR_DETAILS, "last stack");
        flowFile.assertAttributeEquals(ExternalTaskAttributes.EXTENSION_PROPERTY_PREFIX + "targetQueue",
                "priority-high");
    }

    @Test
    public void testHandleFailureWithVariables() throws Exception {
        TestRunner runner = runner(HandleExternalTaskFailure.class);
        runner.setProperty(HandleExternalTaskFailure.VARIABLES_FORMAT, VariablesFormat.SIMPLE);
        runner.setProperty(HandleExternalTaskFailure.LOCAL_VARIABLES, "true");

        runner.enqueue("{\"attempt\": 1}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        List<Object> args = service.lastCall().args();
        assertEquals(7, args.size());
        assertNull(args.get(5));
        assertEquals(json("{\"attempt\": {\"type\": \"Integer\", \"value\": 1}}"),
                json((String) args.get(6)));
    }

    @Test
    public void testUnlockExternalTask() throws Exception {
        TestRunner runner = runner(UnlockExternalTask.class);

        runner.enqueue("", Map.of(ExternalTaskAttributes.ID, "task-99"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExternalTaskProcessor.REL_SUCCESS, 1);
        assertEquals("unlock", service.lastCall().method());
        assertEquals(List.of("task-99"), service.lastCall().args());
    }

    @Test
    public void testVerifyLockDurationTooShort() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        runner.setProperty(FetchAndLockExternalTasks.LOCK_DURATION, "2 sec");

        List<ConfigVerificationResult> results = runner.verify(Map.of());
        assertEquals(2, results.size());

        ConfigVerificationResult lockStep = results.get(0);
        assertEquals("Lock Duration", lockStep.getVerificationStepName());
        assertEquals(Outcome.FAILED, lockStep.getOutcome());
        assertTrue(lockStep.getExplanation().contains("must be at least 3 seconds"));

        ConfigVerificationResult pendingStep = results.get(1);
        assertEquals("Pending Tasks", pendingStep.getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, pendingStep.getOutcome());
    }

    @Test
    public void testVerifySuccessfulWithTasks() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        runner.setProperty(FetchAndLockExternalTasks.LOCK_DURATION, "10 sec");
        service.pendingTaskCount = 5L;

        List<ConfigVerificationResult> results = runner.verify(Map.of());
        assertEquals(2, results.size());

        ConfigVerificationResult lockStep = results.get(0);
        assertEquals("Lock Duration", lockStep.getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, lockStep.getOutcome());

        ConfigVerificationResult pendingStep = results.get(1);
        assertEquals("Pending Tasks", pendingStep.getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, pendingStep.getOutcome());
        assertTrue(pendingStep.getExplanation().contains("Found 5 pending unlocked task(s) ready for topic 'orders'"));

        assertTrue(service.calls.stream().noneMatch(c -> c.method().startsWith("fetchAndLock")));
    }

    @Test
    public void testVerifySuccessfulWithZeroTasks() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        service.pendingTaskCount = 0L;

        List<ConfigVerificationResult> results = runner.verify(Map.of());
        assertEquals(2, results.size());

        ConfigVerificationResult pendingStep = results.get(1);
        assertEquals("Pending Tasks", pendingStep.getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, pendingStep.getOutcome());
        assertTrue(pendingStep.getExplanation().contains("Found 0 pending tasks"));
    }

    @Test
    public void testVerifyPendingTasksUnsupported() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        service.throwUnsupportedOnCount = true;

        List<ConfigVerificationResult> results = runner.verify(Map.of());
        assertEquals(2, results.size());

        ConfigVerificationResult pendingStep = results.get(1);
        assertEquals("Pending Tasks", pendingStep.getVerificationStepName());
        assertEquals(Outcome.SKIPPED, pendingStep.getOutcome());
        assertTrue(pendingStep.getExplanation().contains("does not support task counting"));
    }

    @Test
    public void testVerifyPendingTasksFailure() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "orders");
        service.failure = new OperatonException("Database down", 500);

        List<ConfigVerificationResult> results = runner.verify(Map.of());
        assertEquals(2, results.size());

        ConfigVerificationResult pendingStep = results.get(1);
        assertEquals("Pending Tasks", pendingStep.getVerificationStepName());
        assertEquals(Outcome.FAILED, pendingStep.getOutcome());
        assertTrue(pendingStep.getExplanation().contains("Database down"));
    }

    @Test
    public void testVerifyMissingTopicOrService() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(FetchAndLockExternalTasks.class);
        List<ConfigVerificationResult> results = runner.verify(Map.of());
        assertEquals(2, results.size());
        assertEquals(Outcome.FAILED, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("External Task Service is not configured"));

        MockExternalTaskService mockSvc = new MockExternalTaskService();
        runner.addControllerService("svc", mockSvc);
        runner.enableControllerService(mockSvc);
        runner.setProperty(AbstractExternalTaskProcessor.SERVICE, "svc");
        results = runner.verify(Map.of());
        assertEquals(2, results.size());
        assertEquals(Outcome.FAILED, results.get(1).getOutcome());
        assertTrue(results.get(1).getExplanation().contains("Topic Name is not configured"));
    }

    @Test
    public void testFetchAndLockProvenanceTransitUri() throws Exception {
        TestRunner runner = runner(FetchAndLockExternalTasks.class);
        runner.setProperty(FetchAndLockExternalTasks.TOPIC_NAME, "topic");
        service.transitUriPrefix = "http://localhost:8080/engine-rest/external-task/";
        service.tasks.add(task("task-1", null));

        runner.run();

        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals("http://localhost:8080/engine-rest/external-task/task-1",
                runner.getProvenanceEvents().get(0).getTransitUri());
    }

    @Test
    public void testCompleteExternalTaskProvenanceTransitUri() throws Exception {
        TestRunner runner = runner(CompleteExternalTask.class);
        service.transitUriPrefix = "http://localhost:8080/engine-rest/external-task/";

        runner.enqueue("{}", Map.of(ExternalTaskAttributes.ID, "task-1"));
        runner.run();

        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals("http://localhost:8080/engine-rest/external-task/task-1",
                runner.getProvenanceEvents().get(0).getTransitUri());
    }

    @Test
    public void testDefaultSettingsAnnotation() {
        DefaultSettings defaultSettings = FetchAndLockExternalTasks.class.getAnnotation(DefaultSettings.class);
        assertNotNull(defaultSettings);
        assertEquals("5 sec", defaultSettings.yieldDuration());
    }
}
