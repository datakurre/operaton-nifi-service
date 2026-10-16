# Operaton NiFi External Task Worker

Apache NiFi 2.x extensions for working on [Operaton](https://operaton.org)
external tasks, plus a devenv module for running NiFi locally with Operaton BPM and HashiCorp Vault.

## Components

`nifi-controller/` — **StandardOperatonExternalTaskService**, a controller service
wrapping the Operaton External Task REST API (`/external-task/...`).

| Property | Default | Description |
| --- | --- | --- |
| Operaton REST URL | `http://localhost:8080/engine-rest` | Base URL of the Operaton REST API (`ENVIRONMENT` expression language scope, parameter context compatible) |
| Worker ID | `nifi-${hostname()}` | Unique worker identifier per NiFi node |
| Username / Password | optional | HTTP Basic authentication credentials (`Password` is marked sensitive for Vault / Parameter Contexts) |
| Bearer Token | optional | OAuth2 / JWT Bearer token authentication (sensitive; takes precedence over Basic auth when configured) |
| Request Timeout | `30 sec` | HTTP request timeout (long polling timeout is added on top) |

`nifi-processor/` — processors using that service:

| Processor | Does | Key Properties |
| --- | --- | --- |
| FetchAndLockExternalTasks | Locks tasks of a topic; one FlowFile per task, variables as JSON content, task metadata as `operaton.externalTask.*` attributes | `Topic Name`, `Max Tasks` (`10`), `Lock Duration` (`5 min`), `Long Polling Timeout` (`0 sec`), `Use Priority` (`true`), `Variables`, `Local Variables` (`false`), `Deserialize Values` (`true`), `Include Extension Properties` (`false`), `Business Key`, `Process Definition ID`, `Process Definition Key`, `Process Definition Version Tag`, `Without Tenant ID` (`false`), `Tenant IDs`, `Variables Format` (`Simple JSON`) |
| CompleteExternalTask | Completes the task, setting variables from FlowFile content | `External Task ID` (`${operaton.externalTask.id}`), `Variables Format` (`Simple JSON`), `Local Variables` (`false`) |
| HandleExternalTaskFailure | Reports a failure; decrements `operaton.externalTask.retries` (or uses *Default Retries* on the first failure); 0 retries creates an incident | `External Task ID` (`${operaton.externalTask.id}`), `Error Message` (`${operaton.error.message}`), `Error Details`, `Retries` (optional override), `Default Retries` (`3`), `Retry Timeout` (`1 min`), `Variables Format` (`None`), `Local Variables` (`false`) |
| HandleExternalTaskBpmnError | Throws a BPMN error with an error code to be caught by an error boundary event or event sub-process | `External Task ID` (`${operaton.externalTask.id}`), `Error Code`, `Error Message`, `Variables Format` (`None`) |
| ExtendExternalTaskLock | Extends the lock before long-running steps | `External Task ID` (`${operaton.externalTask.id}`), `Lock Duration` (`5 min`) |
| UnlockExternalTask | Releases the lock on an external task so another worker can pick it up immediately | `External Task ID` (`${operaton.externalTask.id}`) |

The task ID is read from `${operaton.externalTask.id}` by default. When an API call fails, the FlowFile is routed to `failure` with `operaton.error.status` (HTTP status code, `0` if connection/client error) and `operaton.error.message`, so a failed *Complete* or *BpmnError* can be routed straight to *HandleExternalTaskFailure*.

### Flow Patterns

1. **Standard Processing**:
   ```
   FetchAndLockExternalTasks → (your processing) → CompleteExternalTask
                                                          │ (failure)
                                                          ▼
                                              HandleExternalTaskFailure
   ```

2. **BPMN Business Errors vs Technical Failures**:
   ```
   FetchAndLockExternalTasks → (validation / business logic)
                                    ├── [valid]   → CompleteExternalTask
                                    │                     │ (failure)
                                    │                     ▼
                                    │             HandleExternalTaskFailure
                                    └── [invalid] → HandleExternalTaskBpmnError (e.g. Error Code "${error.code}")
                                                          │ (failure)
                                                          ▼
                                                  HandleExternalTaskFailure
   ```
   - **BPMN Errors (`HandleExternalTaskBpmnError`)**: Represents modeled domain/business exceptions caught by BPMN boundary events or event sub-processes in Operaton. FlowFile content can optionally supply variables with the error.
   - **Technical Failures (`HandleExternalTaskFailure`)**: Represents unexpected processing exceptions or upstream service failures. Decrements remaining retries, optionally passes variables/localVariables, and creates an Operaton incident once retries reach 0.

### Attributes Reference

`FetchAndLockExternalTasks` writes the following attributes to each FlowFile:

| Attribute | Description |
| --- | --- |
| `operaton.externalTask.id` | External task ID |
| `operaton.externalTask.topicName` | External task topic name |
| `operaton.externalTask.workerId` | Worker ID holding the lock |
| `operaton.externalTask.activityId` | BPMN activity ID (task definition) |
| `operaton.externalTask.activityInstanceId` | Execution activity instance ID |
| `operaton.externalTask.executionId` | Execution ID |
| `operaton.externalTask.processInstanceId` | Process instance ID |
| `operaton.externalTask.processDefinitionId` | Process definition ID |
| `operaton.externalTask.processDefinitionKey` | Process definition key |
| `operaton.externalTask.processDefinitionVersionTag` | Process definition version tag (if set) |
| `operaton.externalTask.businessKey` | Process instance business key (if set) |
| `operaton.externalTask.tenantId` | Tenant ID (if set) |
| `operaton.externalTask.retries` | Remaining retries count (omitted on first attempt) |
| `operaton.externalTask.priority` | Task priority |
| `operaton.externalTask.createTime` | ISO timestamp of task creation |
| `operaton.externalTask.lockExpirationTime` | ISO timestamp of lock expiration |
| `operaton.externalTask.errorMessage` | Error message from previous failure attempt (if any) |
| `operaton.externalTask.errorDetails` | Error details from previous failure attempt (if any) |
| `operaton.externalTask.extensionProperty.<name>` | BPMN extension property `<name>` (when `Include Extension Properties` is `true`) |
| `operaton.externalTask.variableTypes` | Saved JSON type mapping (for Simple JSON mode round-tripping) |
| `mime.type` | `application/json` |

Error attributes set when processors route to `failure`:

| Attribute | Description |
| --- | --- |
| `operaton.error.status` | HTTP status code returned by the REST API (`0` if connection or client error) |
| `operaton.error.message` | Error message returned by Operaton or the client exception message |

### Process Variables in FlowFiles

Variables in FlowFile content are either:

- **Simple JSON** (default): `{"name": "Ada", "count": 2, "order": {"id": 1}}`.
  Fetch records the original variable types in the `operaton.externalTask.variableTypes` attribute, and Complete / BPMN Error / Failure restore them when the value still fits (so `Long`, `Short`, `Date`, `Xml` and serialized `Object` variables survive a round trip). `File` variables are never written back. Other values are typed by their JSON value: strings, booleans, integers (`Integer`/`Long`), decimals (`Double`) and nulls map to the matching Operaton types; numbers that `Long` or `Double` cannot hold exactly are rejected; objects and arrays map to `Json` variables, so the engine needs the Spin JSON plugin.
- **Operaton Typed JSON**: `{"name": {"type": "String", "value": "Ada"}}`, as used by the REST API directly.

### HashiCorp Vault Integration

`StandardOperatonExternalTaskService` properties support NiFi Parameter Contexts backed by `HashiCorpVaultParameterProvider`:

1. **Vault KV-v2 Secret (`secret/operaton`)**: Stores `url`, `worker_id`, `username`, and `password`.
2. **`StandardHashiCorpVaultClientService`**: Configured with `configuration-strategy = direct-properties` and connected to Vault (`http://127.0.0.1:8200`).
3. **`HashiCorpVaultParameterProvider`**: Reads secrets from KV-v2 mount `secret` and synchronizes them into Parameter Context `operaton` (`password` as a sensitive parameter; `url`, `worker_id`, and `username` as non-sensitive parameters).
4. **`StandardOperatonExternalTaskService`**: References `#{url}`, `#{worker_id}`, `#{username}`, and `#{password}` from the bound `operaton` Parameter Context.

## Demo Process & Local Development

The repository includes a sample BPMN process in `deployment/demo-process.bpmn` (`demo-process` with an external service task on topic `demo`).

Start the entire local development stack (Operaton, NiFi, Vault, and auto-preconfiguration):

```sh
devenv up
```

Services started:
- **Operaton BPM**: http://127.0.0.1:8080 (REST API at `/engine-rest` with Basic Auth enabled, Webapps at `/operaton/`, credentials: `admin` / `admin`, auto-deploys `deployment/demo-process.bpmn`)
- **Apache NiFi**: http://127.0.0.1:8081/nifi/ (Plain HTTP, no authentication)
- **HashiCorp Vault**: http://127.0.0.1:8200 (Web UI enabled, unsealed automatically, token stored in `.devenv/state/env_file`)
- **Preconfiguration**: Automatically provisions Vault KV secrets (`secret/operaton`), synchronizes `HashiCorpVaultParameterProvider` into Parameter Context `operaton`, configures and enables `StandardOperatonExternalTaskService` using Vault parameters (`#{url}`, `#{worker_id}`, `#{username}`, `#{password}`), sets up the `FetchAndLockExternalTasks` → `CompleteExternalTask` → `HandleExternalTaskFailure` flow on the NiFi canvas, and starts a `demo-process` instance waiting on topic `demo`.

Additional commands:
```sh
devenv shell -- build          # mvn package for all modules, NARs under */*-nar/target
devenv test                    # run unit tests across all controller and processor modules
devenv shell -- nifi-deploy    # manually copy newly rebuilt NARs into NiFi's autoload directory
devenv shell -- preconfigure   # rerun preconfiguration on demand
devenv shell -- vault status   # interact with local Vault using vault CLI
```

The `services.nifi` module (`devenv/modules/nifi.nix`) packages the official
NiFi 2.4.0 binary, because nixpkgs only has NiFi 1.x. It runs NiFi over plain
HTTP with no authentication (local development only) and keeps its state in
`.devenv/state/nifi`. Built NARs in `narPaths` are automatically built if missing and deployed to
NiFi's autoload directory on startup.

## Visual Documentation & Screencasts

Automated headless browser scripts generate screenshots and multi-actor walkthrough screencasts of the running stack:

- **Generate screenshots** (NiFi canvas, Operaton Cockpit & Tasklist, Vault):
  ```sh
  playwright-python docs/scripts/capture_screenshots.py
  # Optionally record video clip:
  playwright-python docs/scripts/capture_screenshots.py --record-video
  ```
- **Multi-actor screencast walkthrough** (driven with [robotframework-screencast](https://github.com/datakurre/robotframework-screencast)):
  ```sh
  screencast run docs/screencast/external_task_walkthrough.robot --take docs/screencast/take-1
  screencast compose docs/screencast/take-1
  screencast verify docs/screencast/take-1
  ```

## GitHub Codespaces

This repository includes a `.devcontainer.json` configuration based on the devenv devcontainer image (`ghcr.io/cachix/devenv/devcontainer:latest`, currently `v2.4.0`) with Nix, Direnv, devenv, Java, and Operaton BPMN/DMN/Form modeler extensions pre-configured.

When launched in Codespaces:
- `updateContentCommand` automatically builds the NAR extensions via `devenv shell -- build`.
- Operaton BPMN Modeler (`datakurre.vscode-operaton-bpmn-js-modeler`) is pre-configured with `bpmnModeler.engineRestUrl = http://localhost:8080/engine-rest` for one-click deployment.
- Ports 8080 (Operaton), 8081 (NiFi), and 8200 (Vault) are forwarded automatically.
- NiFi (`nifi.web.proxy.host`, including dynamic `${CODESPACE_NAME}-8081.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}` entries) and Operaton (`forwardHeadersStrategy = "native"`) are configured to support Codespaces' forwarded reverse-proxy URLs out of the box.
- Run `devenv up` inside the terminal to boot the full stack.
