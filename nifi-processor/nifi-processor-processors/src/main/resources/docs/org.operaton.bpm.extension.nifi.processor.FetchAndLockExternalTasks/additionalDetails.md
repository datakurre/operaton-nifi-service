# FetchAndLockExternalTasks

## Description

`FetchAndLockExternalTasks` polls the Operaton REST API for external tasks belonging to a specified topic. When tasks are available, the processor locks them for a worker ID and emits one FlowFile per task. The FlowFile content contains the fetched process variables formatted as JSON, and metadata attributes identifying the BPMN task and process instance are written to FlowFile attributes.

Downstream processors can transform or inspect the variables before sending the task to one of the terminating processors:
- `CompleteExternalTask`: successfully finishes the task with updated process variables.
- `HandleExternalTaskBpmnError`: reports a business error (`BpmnError`) caught by error boundary events in BPMN.
- `HandleExternalTaskFailure`: reports technical failures, optionally decrementing retries or opening an incident.
- `UnlockExternalTask`: unlocks the task so another worker or process can claim it immediately.

## Variables Formats

The **Variables Format** property controls how process variables are serialized into FlowFile content:

- **`Simple JSON` (`SIMPLE`)** (Default): Variables are formatted as a direct JSON key-value object:
  ```json
  {
    "orderId": "12345",
    "itemCount": 3,
    "approved": true,
    "customer": {
      "name": "Ada Lovelace"
    }
  }
  ```
  This format is directly consumable by standard NiFi JSON processors such as `JSLTTransformJSON`, `EvaluateJsonPath`, `SplitJson`, and `QueryRecord`.

- **`Operaton Typed JSON` (`TYPED`)**: Variables are formatted in the Operaton REST API typed representation:
  ```json
  {
    "orderId": {"type": "String", "value": "12345", "valueInfo": {}},
    "itemCount": {"type": "Integer", "value": 3, "valueInfo": {}},
    "approved": {"type": "Boolean", "value": true, "valueInfo": {}},
    "customer": {"type": "Json", "value": "{\"name\":\"Ada Lovelace\"}", "valueInfo": {}}
  }
  ```
  Use this format when precise BPMN engine type metadata must be preserved throughout processing.

## Automatic Lock Extension

When an external task is locked with a duration longer than 3 seconds (the default is 20 seconds), the processor automatically registers the task with the referenced `OperatonExternalTaskService`.

- **Heartbeat Extension**: A background thread in the controller service periodically extends the locks of active tasks before their lock expires (halfway through the remaining duration).
- **Completion Deregistration**: When `CompleteExternalTask`, `HandleExternalTaskFailure`, `HandleExternalTaskBpmnError`, or `UnlockExternalTask` runs, the task is deregistered from the lock extender.
- **Fail-safe Ceiling**: To prevent abandoned tasks from remaining locked indefinitely if a NiFi node terminates abnormally or a flow hangs, the service enforces a **Max Lock Lifetime** (default: 1 hour). Once reached, automatic lock extensions cease and the lock is allowed to expire in Operaton.

## FlowFile Attributes

Each FlowFile emitted to `success` contains the following attributes:

| Attribute Name | Description |
|---|---|
| `operaton.externalTask.id` | The unique External Task ID in Operaton |
| `operaton.externalTask.topicName` | Topic name of the external task |
| `operaton.externalTask.workerId` | Worker ID that locked the task |
| `operaton.externalTask.activityId` | BPMN activity ID of the service task |
| `operaton.externalTask.activityInstanceId` | Activity instance ID |
| `operaton.externalTask.executionId` | Execution ID |
| `operaton.externalTask.processInstanceId` | Process Instance ID |
| `operaton.externalTask.processDefinitionId` | Process Definition ID |
| `operaton.externalTask.processDefinitionKey` | Process Definition Key |
| `operaton.externalTask.processDefinitionVersionTag` | Process Definition Version Tag (if present) |
| `operaton.externalTask.businessKey` | Business key of the process instance (if present) |
| `operaton.externalTask.tenantId` | Tenant ID (if multi-tenancy is used) |
| `operaton.externalTask.retries` | Remaining retry count |
| `operaton.externalTask.priority` | Task priority |
| `operaton.externalTask.createTime` | Task creation timestamp |
| `operaton.externalTask.lockExpirationTime` | Lock expiration timestamp (ISO 8601) |
| `operaton.externalTask.errorMessage` | Last reported error message (if any) |
| `operaton.externalTask.errorDetails` | Last reported error details (if any) |
| `operaton.externalTask.variableTypes` | Original Operaton variable types (Simple JSON format) |
| `operaton.externalTask.extensionProperty.<name>` | BPMN extension properties (when Include Extension Properties is true) |
| `mime.type` | Set to `application/json` |

## Scheduling and Polling Guidance

- **Yield Duration**: Configured by default to `5 sec`. When no tasks are available in Operaton or a transient connection issue occurs, the processor yields execution to avoid busy-looping and conserve CPU cycles.
- **Long Polling Timeout**: For high-throughput or low-latency workloads, set **Long Polling Timeout** (e.g. `10 sec` or `30 sec`). Operaton will suspend the HTTP connection until tasks matching the topic arrive. Note that long polling holds an active execution thread during the wait duration.
