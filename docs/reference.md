# Technical Specification & API Reference

This document provides the authoritative technical reference for the **Operaton NiFi External Task Worker** service and processor components.

---

## 1. Architecture & Component Summary

The extension consists of two primary modules:

1. **`nifi-controller/`**: Provides `StandardOperatonExternalTaskService`, a controller service implementing `OperatonExternalTaskService`. It handles REST communication with the Operaton engine (`/external-task/...`), connection pooling, authentication, and background lock extensions.
2. **`nifi-processor/`**: Provides five specialized processors for managing the external task lifecycle within Apache NiFi dataflows.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Operaton Engine REST API                        │
└───────▲──────────────────────────▲───────────────────────────▲─────────┘
        │ /fetchAndLock            │ /complete, /handleFailure │ /extendLock
        ▼                          ▼                           │
┌─────────────────────────┐  ┌────────────────────────────┐    │
│FetchAndLockExternalTasks│  │ Complete / Failure / Error │    │
└───────────┬─────────────┘  └─────────────▲──────────────┘    │
            │ (FlowFile)                   │                   │
            ▼                              │                   │
  [ NiFi Dataflow ] ───────────────────────┘                   │
            │                                                  │
            └───────────► StandardOperatonExternalTaskService ─┘
                           (Automatic Background Lock Renewal)
```

---

## 2. StandardOperatonExternalTaskService

The controller service wraps the Operaton External Task REST API, maintains active locks, and automatically extends leases before expiration.

### Property Reference

| Property | Default | Sensitive | Dynamic | Description |
| :--- | :--- | :---: | :---: | :--- |
| **Operaton REST URL** | `http://localhost:8080/engine-rest` | No | Yes (EL: `ENVIRONMENT`) | Base URL of the Operaton REST API. Compatible with NiFi Parameter Contexts (`#{url}`). |
| **Worker ID** | `nifi-${hostname()}` | No | Yes (EL: `ENVIRONMENT`) | Unique identifier for the worker node holding the task lock. |
| **SSL Context Service** | *(none)* | No | No | SSL Context Service providing trust store (and key store for mutual TLS) for HTTPS connections to Operaton and OAuth2 Token Endpoint. JVM default trust store is used when not set. |
| **Proxy Configuration Service** | *(none)* | No | No | Specifies the Proxy Configuration Controller Service to proxy network requests to Operaton. |
| **Authentication Strategy** | `None` | No | No | How requests to the Operaton REST API are authenticated. Allowable values: `None`, `HTTP Basic`, `Bearer Token`, `OAuth2 Access Token Provider`, `OAuth2 Client Credentials` *(Deprecated)*. Dependent credential properties are displayed and validated based on this selection. |
| **OAuth2 Access Token Provider** | *(none)* | No | No | Controller service reference (`OAuth2AccessTokenProvider`) for managed OAuth2 tokens. Recommended for OAuth2 authentication. Required when Authentication Strategy is `OAuth2 Access Token Provider`. |
| **OAuth2 Token Endpoint** | *(none)* | No | Yes (EL: `ENVIRONMENT`) | *(Deprecated: use OAuth2 Access Token Provider)* Token URL for dynamic `client_credentials` grant authentication. Required when Authentication Strategy is `OAuth2 Client Credentials`. |
| **OAuth2 Client ID** | *(none)* | No | Yes (EL: `ENVIRONMENT`) | *(Deprecated: use OAuth2 Access Token Provider)* OAuth2 Client ID for service account authentication. Required when Authentication Strategy is `OAuth2 Client Credentials`. |
| **OAuth2 Client Secret** | *(none)* | Yes | Yes (EL: `ENVIRONMENT`) | *(Deprecated: use OAuth2 Access Token Provider)* OAuth2 Client Secret (sensitive; for Vault / Parameter Contexts). Required when Authentication Strategy is `OAuth2 Client Credentials`. |
| **OAuth2 Scope** | *(none)* | No | Yes (EL: `ENVIRONMENT`) | *(Deprecated: use OAuth2 Access Token Provider)* Optional scope(s) for the OAuth2 client credentials grant. Shown when Authentication Strategy is `OAuth2 Client Credentials`. |
| **Bearer Token** | *(none)* | Yes | Yes (EL: `ENVIRONMENT`) | Fixed Bearer / JWT token for static authorization headers. Required when Authentication Strategy is `Bearer Token`. |
| **Username** | *(none)* | No | Yes (EL: `ENVIRONMENT`) | HTTP Basic Auth username (`#{username}`). Required when Authentication Strategy is `HTTP Basic`. |
| **Password** | *(none)* | Yes | Yes (EL: `ENVIRONMENT`) | HTTP Basic Auth password (sensitive; `#{password}`). Shown when Authentication Strategy is `HTTP Basic`. |
| **Request Timeout** | `30 sec` | No | No | HTTP request timeout for REST API invocations (long polling timeout is added on top). |
| **Max Lock Lifetime** | `1 hour` | No | No | Upper bound on how long a single task lock can be continuously extended. |

> [!NOTE]
> **Proxy Authentication Note:** The JDK `HttpClient` disables Basic authentication for HTTPS tunneling (`CONNECT`) by default (`jdk.http.auth.tunneling.disabledSchemes`). For proxies requiring Basic authentication, plain HTTP to Operaton is supported; for HTTPS through an authenticating proxy, ensure the JVM's `jdk.http.auth.tunneling.disabledSchemes` networking property is configured accordingly or use an unauthenticated proxy.

### Configuring OAuth2 Authentication (Recommended)

To authenticate requests to Operaton with OAuth2 using client credentials, configure a `StandardOauth2AccessTokenProvider` controller service (provided by NiFi's `nifi-oauth2-provider-nar`):
1. Add a **StandardOauth2AccessTokenProvider** controller service.
2. Configure its properties:
   - **Grant Type**: `client_credentials`
   - **Authorization Server URL**: URL of the token endpoint, e.g. `https://auth.example.com/oauth/token`
   - **Client ID**: OAuth2 client identifier
   - **Client Secret**: OAuth2 client secret
   - **Scope**: *(optional)* Scope(s) required by Operaton
   - **SSL Context Service**: *(optional)* SSL context service if the token endpoint uses private CA certificates
   - **Proxy Configuration Service**: *(optional)* Proxy configuration if token endpoint is behind an HTTP proxy
3. In `StandardOperatonExternalTaskService`:
   - Set **Authentication Strategy** to `OAuth2 Access Token Provider`.
   - Set **OAuth2 Access Token Provider** to reference the configured service.

> [!WARNING]
> **OAuth2 Client Credentials Deprecation**: Direct OAuth2 service account configuration via the built-in `OAuth2 Client Credentials` strategy is deprecated in favor of `StandardOauth2AccessTokenProvider` and will be removed in a future release. Existing configurations will continue to function.

### Automatic Background Lock Extension Engine

To prevent tasks from timing out during long-running batch, database, or external API execution, `StandardOperatonExternalTaskService` manages background renewals automatically:

- **Half-Life Renewal**: The service tracks every task fetched through `FetchAndLockExternalTasks`. Once **half of the task's fetch `Lock Duration`** has elapsed, the service extends the lock by that duration again.
- **Termination Triggers**: Extension stops immediately when:
  1. The task is completed via `CompleteExternalTask`.
  2. The task fails via `HandleExternalTaskFailure`.
  3. A BPMN business error is thrown via `HandleExternalTaskBpmnError`.
  4. The task is explicitly released via `UnlockExternalTask`.
  5. Operaton returns HTTP `400` or `404` (indicating the task was completed elsewhere or expired).
  6. Elapsed time reaches **`Max Lock Lifetime`** (protects against orphaned FlowFiles dropped by auto-terminated relationships or expired queues).
- **Cluster & Worker Affinity**: Task locks in Operaton are bound to a specific `Worker ID`. The node that fetched the task performs all renewals.
- **State Persistence**: Tracked tasks are stored in the service's local state, allowing extensions to resume seamlessly following a clean NiFi restart.

---

## 3. Processors Reference

### 3.1. FetchAndLockExternalTasks
Polls Operaton for external tasks matching a configured topic, acquires locks, and outputs one FlowFile per task.

- **Input / Output**: Does not accept incoming connections. Generates FlowFiles on `success`.
- **Relationships**: `success` (locked tasks emitted as FlowFiles).
- **Properties**:

| Property | Default | Required | Description |
| :--- | :--- | :---: | :--- |
| **External Task Service** | *(none)* | Yes | Reference to `StandardOperatonExternalTaskService`. |
| **Topic Name** | *(none)* | Yes | Operaton topic name to subscribe to. Supports Expression Language. |
| **Max Tasks** | `10` | Yes | Maximum number of tasks to lock in a single fetch batch. |
| **Lock Duration** | `5 min` | Yes | Initial lock lease duration. Automatically renewed by the service. |
| **Variables Format** | `Simple JSON` | Yes | Format for FlowFile content: `Simple JSON` or `Operaton Typed JSON`. |
| **Variables** | *(none)* | No | Comma-separated list of specific variable names to fetch (fetches all if omitted). |
| **Long Polling Timeout** | `0 sec` | Yes | Enables server-side long polling (up to `180 sec`) to avoid busy-waiting. |
| **Use Priority** | `true` | Yes | Fetches higher-priority tasks first. |
| **Local Variables** | `false` | Yes | Fetches task-local variables instead of process instance scope. |
| **Deserialize Values** | `true` | Yes | Deserializes Java objects into native JSON representations. |
| **Include Extension Properties** | `false` | Yes | Emits BPMN model extension properties as FlowFile attributes. |
| **Business Key** | *(none)* | No | Filters tasks by process instance business key. |
| **Process Definition Key** | *(none)* | No | Filters tasks by BPMN process definition key. |
| **Tenant IDs** | *(none)* | No | Comma-separated list of tenant IDs to filter by. |

---

### 3.2. CompleteExternalTask
Completes an active external task in Operaton, passing variables from FlowFile content back to the process instance.

- **Relationships**: `success` (task completed), `failure` (REST call or connection error).
- **Properties**:

| Property | Default | Required | Description |
| :--- | :--- | :---: | :--- |
| **External Task Service** | *(none)* | Yes | Reference to `StandardOperatonExternalTaskService`. |
| **External Task ID** | `${operaton.externalTask.id}` | Yes | ID of the task to complete. Supports Expression Language. |
| **Variables Format** | `Simple JSON` | Yes | Format of variables in FlowFile content (`Simple JSON`, `Operaton Typed JSON`, or `None`). |
| **Local Variables** | `false` | Yes | Submits variables to task-local scope instead of process instance scope. |

---

### 3.3. HandleExternalTaskFailure
Reports a technical failure to Operaton. Decrements the remaining retry count and, once retries reach 0, creates an incident in Operaton Cockpit.

- **Relationships**: `success` (failure recorded), `failure` (error contacting Operaton).
- **Properties**:

| Property | Default | Required | Description |
| :--- | :--- | :---: | :--- |
| **External Task Service** | *(none)* | Yes | Reference to `StandardOperatonExternalTaskService`. |
| **External Task ID** | `${operaton.externalTask.id}` | Yes | ID of the failed task. |
| **Error Message** | `${operaton.error.message}` | Yes | Short failure summary displayed in Cockpit. |
| **Error Details** | *(none)* | No | Full stack trace or verbose error details. |
| **Default Retries** | `3` | Yes | Initial retry count applied on the first failure attempt. |
| **Retries** | *(none)* | No | Explicit override for remaining retries (defaults to decrementing previous count). |
| **Retry Timeout** | `1 min` | Yes | Delay before Operaton allows the task to be fetched again. |
| **Variables Format** | `None` | Yes | Format of variables to update during failure (`None`, `Simple JSON`, `Typed JSON`). |

---

### 3.4. HandleExternalTaskBpmnError
Throws a domain/business error that is caught by an attached **BPMN Error Boundary Event** or Event Subprocess.

- **Relationships**: `success` (error thrown), `failure` (error contacting Operaton).
- **Properties**:

| Property | Default | Required | Description |
| :--- | :--- | :---: | :--- |
| **External Task Service** | *(none)* | Yes | Reference to `StandardOperatonExternalTaskService`. |
| **External Task ID** | `${operaton.externalTask.id}` | Yes | ID of the external task. |
| **Error Code** | *(none)* | Yes | Business error code matched by BPMN boundary events (e.g. `UNKNOWN_DESTINATION`). |
| **Error Message** | *(none)* | No | Human-readable explanation of the business exception. |
| **Variables Format** | `None` | Yes | Format of variables attached to the BPMN error. |

---

### 3.5. UnlockExternalTask
Releases the lock on an external task without recording a failure or decrementing retries, allowing another worker node or pipeline to claim it immediately.

- **Relationships**: `success`, `failure`.
- **Properties**:

| Property | Default | Required | Description |
| :--- | :--- | :---: | :--- |
| **External Task Service** | *(none)* | Yes | Reference to `StandardOperatonExternalTaskService`. |
| **External Task ID** | `${operaton.externalTask.id}` | Yes | ID of the external task to unlock. |

---

## 4. Flow Patterns & Best Practices

### Pattern 1: Standard Pipeline Execution
```
FetchAndLockExternalTasks ──> [ Transform / API Call ] ──> CompleteExternalTask
                                                                   │ (failure)
                                                                   ▼
                                                       HandleExternalTaskFailure
```
Route the `failure` relationship of `CompleteExternalTask` directly to `HandleExternalTaskFailure` to guarantee that unexpected network blips during task completion trigger automatic retries.

### Pattern 2: Domain Errors vs. Technical Incidents
```
FetchAndLockExternalTasks ──> [ Business Validation ]
                                    ├── [Valid]   ──> CompleteExternalTask
                                    │                        │ (failure)
                                    │                        ▼
                                    │                HandleExternalTaskFailure
                                    └── [Invalid] ──> HandleExternalTaskBpmnError
                                                             │ (failure)
                                                             ▼
                                                     HandleExternalTaskFailure
```
- **Business Errors (`HandleExternalTaskBpmnError`)**: Use for domain exceptions modeled in the BPMN diagram (e.g. invalid user input, unserviceable postal code). Diverts process tokens along alternative modeled paths without raising operational alarms.
- **Technical Failures (`HandleExternalTaskFailure`)**: Use for infrastructure outages, timeouts, or unhandled exceptions. Decrements retries and generates actionable incidents in Operaton Cockpit once retries are exhausted.

---

## 5. FlowFile Attributes Reference

### Task Attributes Emitted by `FetchAndLockExternalTasks`

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `operaton.externalTask.id` | String | Unique External Task instance ID. |
| `operaton.externalTask.topicName` | String | Subscribed topic name. |
| `operaton.externalTask.workerId` | String | Worker holding the lease. |
| `operaton.externalTask.activityId` | String | BPMN XML element ID of the service task. |
| `operaton.externalTask.activityInstanceId`| String | Runtime activity instance ID. |
| `operaton.externalTask.executionId` | String | Execution token ID. |
| `operaton.externalTask.processInstanceId` | String | Process instance ID. |
| `operaton.externalTask.processDefinitionId` | String | Process definition ID. |
| `operaton.externalTask.processDefinitionKey` | String | BPMN process key (e.g. `finnish-travel-planner`). |
| `operaton.externalTask.processDefinitionVersionTag` | String | Process version tag (if configured). |
| `operaton.externalTask.businessKey` | String | Process instance business key. |
| `operaton.externalTask.tenantId` | String | Tenant ID (in multi-tenant environments). |
| `operaton.externalTask.retries` | Integer | Remaining retry attempts. |
| `operaton.externalTask.priority` | Long | Task priority. |
| `operaton.externalTask.createTime` | String | ISO-8601 creation timestamp. |
| `operaton.externalTask.lockExpirationTime`| String | ISO-8601 expiration timestamp. |
| `operaton.externalTask.errorMessage` | String | Error message from prior attempt (if retried). |
| `operaton.externalTask.errorDetails` | String | Full error details from prior attempt (if retried). |
| `operaton.externalTask.variableTypes` | String | Internal type mapping for round-trip type preservation. |
| `mime.type` | String | `application/json` |

### Error Attributes Emitted on `failure`

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `operaton.error.status` | Integer | HTTP response status code (`0` for connection/network failures). |
| `operaton.error.message` | String | Error message returned by Operaton or the client exception. |

---

## 6. Process Variables & Type Serialization

### Serialization Modes

1. **`Simple JSON` (Default)**:
   Emits standard, clean JSON objects into FlowFile content:
   ```json
   {
     "destination": "Koli",
     "temperature": -1.5,
     "confirmed": true,
     "bookingCount": 4
   }
   ```
   - **Type Preservation**: `FetchAndLockExternalTasks` records original BPMN types in `operaton.externalTask.variableTypes`. When `CompleteExternalTask` completes the task, it restores types that JSON cannot natively represent (e.g. `Long`, `Short`, `Date`, `Xml`, serialized Java `Object`).
   - Standard JSON values map naturally: numbers map to `Integer`, `Long`, or `Double`; booleans map to `Boolean`; strings map to `String`; arrays and objects map to `Json` (Spin plugin required in Operaton).

2. **`Operaton Typed JSON`**:
   Emits the raw Operaton REST format:
   ```json
   {
     "destination": { "type": "String", "value": "Koli" },
     "temperature": { "type": "Double", "value": -1.5 }
   }
   ```

---

## 7. HashiCorp Vault Integration

The service natively integrates with `HashiCorpVaultParameterProvider` to securely synchronize credentials into NiFi Parameter Contexts:

1. **Vault Secret Mount**: Secrets are stored in Vault KV-v2 at `secret/operaton`:
   - `url`: `http://localhost:8080/engine-rest`
   - `worker_id`: `nifi-worker`
   - `username`: `demo`
   - `password`: `demo` (marked sensitive)
2. **Controller Service Binding**: `StandardHashiCorpVaultClientService` connects to Vault (`http://127.0.0.1:8200`) using direct tokens or AppRole.
3. **Parameter Context Mapping**: NiFi Parameter Context `operaton` maps secrets to parameters:
   - `Operaton REST URL` → `#{url}`
   - `Worker ID` → `#{worker_id}`
   - `Username` → `#{username}`
   - `Password` → `#{password}`

---

## 8. Deployment & Environment Reference

### Local Development via `devenv`
The project's `devenv.nix` provides a fully reproducible environment:
```sh
devenv up -d         # Starts Operaton, NiFi 2.11.0, Vault, and preconfiguration
devenv test          # Runs Maven unit test suite across all modules
devenv shell -- build # Builds NAR archives under */*-nar/target/
```

### Standalone NiFi 2.x Installation
To deploy the extension into an existing Apache NiFi 2.x installation:
1. Copy the built NAR files into NiFi's `lib/` or custom `extensions/` directory:
   - `operaton-nifi-service-api-nar-*.nar`
   - `operaton-nifi-service-nar-*.nar`
   - `operaton-nifi-processors-nar-*.nar`
2. Restart NiFi.
