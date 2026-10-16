package org.operaton.bpm.extension.nifi.processor;

/**
 * FlowFile attributes written and read by the external task processors.
 */
public final class ExternalTaskAttributes {

    public static final String PREFIX = "operaton.externalTask.";

    public static final String ID = PREFIX + "id";
    public static final String TOPIC_NAME = PREFIX + "topicName";
    public static final String WORKER_ID = PREFIX + "workerId";
    public static final String ACTIVITY_ID = PREFIX + "activityId";
    public static final String ACTIVITY_INSTANCE_ID = PREFIX + "activityInstanceId";
    public static final String EXECUTION_ID = PREFIX + "executionId";
    public static final String PROCESS_INSTANCE_ID = PREFIX + "processInstanceId";
    public static final String PROCESS_DEFINITION_ID = PREFIX + "processDefinitionId";
    public static final String PROCESS_DEFINITION_KEY = PREFIX + "processDefinitionKey";
    public static final String PROCESS_DEFINITION_VERSION_TAG = PREFIX + "processDefinitionVersionTag";
    public static final String BUSINESS_KEY = PREFIX + "businessKey";
    public static final String TENANT_ID = PREFIX + "tenantId";
    public static final String RETRIES = PREFIX + "retries";
    public static final String PRIORITY = PREFIX + "priority";
    public static final String CREATE_TIME = PREFIX + "createTime";
    public static final String LOCK_EXPIRATION_TIME = PREFIX + "lockExpirationTime";
    public static final String TASK_ERROR_MESSAGE = PREFIX + "errorMessage";
    public static final String TASK_ERROR_DETAILS = PREFIX + "errorDetails";
    public static final String VARIABLE_TYPES = PREFIX + "variableTypes";
    public static final String EXTENSION_PROPERTY_PREFIX = PREFIX + "extensionProperty.";

    public static final String ERROR_STATUS = "operaton.error.status";
    public static final String ERROR_MESSAGE = "operaton.error.message";

    private ExternalTaskAttributes() {
    }
}
