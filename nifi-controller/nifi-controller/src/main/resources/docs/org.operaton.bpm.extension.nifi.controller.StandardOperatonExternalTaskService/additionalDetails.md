# StandardOperatonExternalTaskService

## Description

`StandardOperatonExternalTaskService` provides shared connectivity, authentication, and automatic lock management for Operaton external task processors (`FetchAndLockExternalTasks`, `CompleteExternalTask`, `HandleExternalTaskFailure`, `HandleExternalTaskBpmnError`, and `UnlockExternalTask`).

## Authentication Strategies

The service supports multiple authentication strategies configured via the **Authentication Strategy** property:

- **None (`NONE`)**: Sends requests without authentication headers. Useful for local development environments or when authentication is handled transparently by an upstream reverse proxy.
- **Basic Authentication (`BASIC`)**: Sends standard HTTP `Authorization: Basic <base64>` credentials using the configured **Username** and **Password**.
- **Bearer Token (`BEARER`)**: Sends a static token in an `Authorization: Bearer <token>` header.
- **OAuth2 Access Token Provider (`OAUTH2_PROVIDER`)**: Delegates token management to an `OAuth2AccessTokenProvider` controller service (such as `StandardOauth2AccessTokenProvider`). This is the recommended approach for OAuth 2.0 Client Credentials and JWT bearer workflows. The service automatically retrieves access tokens and handles transparent token refresh when Operaton returns an HTTP 401 Unauthorized response.

## Automatic Lock Extension and Max Lock Lifetime

Operaton external task locks are granted for a fixed duration. If complex NiFi processing takes longer than the initial lock duration, the lock could expire, causing Operaton to reassign the task to another worker and leading to duplicate processing.

To prevent this:
1. When `FetchAndLockExternalTasks` locks tasks for longer than 3 seconds, they are registered with this service.
2. A single background scheduler thread checks registered tasks and sends an `extendLock` request to Operaton when approximately half of the remaining lock duration has elapsed.
3. When the task is resolved downstream (`CompleteExternalTask`, `HandleExternalTaskFailure`, `HandleExternalTaskBpmnError`, or `UnlockExternalTask`), it is immediately unregistered.
4. **Max Lock Lifetime** (default: `1 hour`): Acts as a fail-safe ceiling. If a FlowFile is stuck in a queue or lost due to an unhandled failure, automatic extensions stop once the cumulative lock duration exceeds this limit, allowing Operaton to release and reassign the task.

## Cluster Deployment and Worker ID Guidance

Operaton tracks task ownership by **Worker ID**. If a node attempts to complete or extend a task locked by a different Worker ID, Operaton rejects the request with an error.

In a multi-node Apache NiFi cluster:
- Each cluster node should use a distinct Worker ID.
- You can configure the **Worker ID** property dynamically using NiFi Expression Language, for example:
  ```
  nifi-${hostname(true)}
  ```
  or by using an environment-specific variable:
  ```
  ${operaton.worker.id:replaceEmpty('nifi-worker')}
  ```
- This ensures that if FlowFiles are routed or load-balanced across cluster nodes, tasks remain associated with the locking node, or lock extension and completion requests are routed consistently.

## TLS and HTTP Proxy Configuration

- **SSL Context Service**: Connects to HTTPS Operaton endpoints using a configured `SSLContextProvider` for custom truststores, keystores, and mutual TLS (mTLS).
- **Proxy Configuration Service**: Routes traffic through HTTP forward proxies via a standard `ProxyConfigurationService`. Pre-emptive proxy authentication is automatically applied for plain-HTTP endpoints.
