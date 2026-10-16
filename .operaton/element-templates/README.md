# Operaton NiFi Element Templates

Element templates for configuring BPMN 2.0 external service tasks compatible with Apache NiFi and the Operaton NiFi Worker extension (`operaton-nifi-service`).

These templates conform to the Operaton Element Templates Schema (version 0.9.1) used by [vscode-operaton-bpmn-js-modeler](https://gitlab.com/vasara-bpm/vscode-operaton-bpmn-js-modeler).

## Available Templates

### 1. NiFi External Task (`org.operaton.nifi.external-task`)

The general-purpose element template for BPMN `ServiceTask` elements executed by NiFi.

- **Topic Name** (`camunda:topic`): Subscribed to by NiFi's `FetchAndLockExternalTasks` processor.
- **Task Priority** (`camunda:taskPriority`): Priority used when `Use Priority` is enabled in `FetchAndLockExternalTasks`.
- **Action & Routing Key** (`camunda:property`): Extension properties emitted into FlowFiles as attributes:
  - `operaton.externalTask.extensionProperty.action`
  - `operaton.externalTask.extensionProperty.routingKey`
- **Variables Format** (`camunda:property`): Informs NiFi processors (`Simple JSON` or `Operaton Typed JSON`).
- **Input Parameters** (`camunda:additionalInputParameters`): Custom task-level variables mapped into the FlowFile content payload.
- **Asynchronous Continuation**: Pre-configured with `asyncBefore=true` and `exclusive=true` for reliable transaction demarcation.

### 2. NiFi External Task with BPMN Error (`org.operaton.nifi.external-task-with-error`)

Configures an external task with a pre-bound `bpmn:Error` definition:

- Includes all properties from the standard task template.
- Pre-binds `camunda:errorEventDefinition` referencing a scoped `bpmn:Error` with:
  - **Error Code** (`errorCode`): Matched by NiFi's `HandleExternalTaskBpmnError` processor (`NIFI_ERROR` default).
  - **Error Message** (`camunda:errorMessage`): Catchable by attached boundary error events or event sub-processes.

### 3. NiFi Demo Task (`org.operaton.nifi.demo-task`)

Pre-configured for local testing with the repository's sample pipeline (`demo-process.bpmn`):

- **Topic Name**: `demo`
- **Action**: `demo-process`
- **Asynchronous Before**: `true`
