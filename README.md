# Operaton NiFi External Task Worker

Apache NiFi 2.x extensions for working on [Operaton](https://operaton.org)
external tasks, plus a devenv module for running NiFi locally.

## Components

`nifi-controller/` — **StandardOperatonExternalTaskService**, a controller service
wrapping the Operaton External Task REST API (`/external-task/...`).

| Property | Default |
| --- | --- |
| Operaton REST URL | `http://localhost:8080/engine-rest` |
| Worker ID | `nifi-${hostname()}` (keep unique per NiFi node) |
| Username / Password | HTTP basic auth, optional |
| Request Timeout | `30 sec` (long polling time is added on top) |

`nifi-processor/` — processors using that service:

| Processor | Does | Key Properties |
| --- | --- | --- |
| FetchAndLockExternalTasks | Locks tasks of a topic; one FlowFile per task, variables as JSON content, task metadata as `operaton.externalTask.*` attributes | `Topic Name`, `Max Tasks` (`10`), `Lock Duration` (`5 min`), `Variables`, `Long Polling Timeout` (`0 sec`), `Variables Format` (`Simple JSON`) |
| CompleteExternalTask | Completes the task, setting variables from FlowFile content | `Variables Format` (`Simple JSON`), `Local Variables` (`false`) |
| HandleExternalTaskFailure | Reports a failure; decrements `operaton.externalTask.retries` (or uses *Default Retries* on the first failure); 0 retries creates an incident | `Error Message`, `Error Details`, `Retries`, `Default Retries` (`3`), `Retry Timeout` (`1 min`) |
| HandleExternalTaskBpmnError | Throws a BPMN error with an error code to be caught by an error boundary event or event sub process | `Error Code`, `Error Message`, `Variables Format` (`None`) |
| ExtendExternalTaskLock | Extends the lock before long running steps | `Lock Duration` (`5 min`) |

The task id is read from `${operaton.externalTask.id}` by default. When an API call fails, the FlowFile is routed to `failure` with `operaton.error.status` (HTTP status code, 0 if connection/client error) and `operaton.error.message`, so a failed *Complete* or *BpmnError* can be routed straight to *HandleExternalTaskFailure*.

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
   - **Technical Failures (`HandleExternalTaskFailure`)**: Represents unexpected processing exceptions or upstream service failures. Decrements remaining retries and creates an Operaton incident once retries reach 0.

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
| `operaton.externalTask.businessKey` | Process instance business key (if set) |
| `operaton.externalTask.tenantId` | Tenant ID (if set) |
| `operaton.externalTask.retries` | Remaining retries count (`null` on first attempt) |
| `operaton.externalTask.priority` | Task priority |
| `operaton.externalTask.lockExpirationTime` | ISO timestamp of lock expiration |
| `operaton.externalTask.variableTypes` | Saved JSON type mapping (for Simple JSON mode round-tripping) |
| `mime.type` | `application/json` |

Error attributes set when processors route to `failure`:

| Attribute | Description |
| --- | --- |
| `operaton.error.status` | HTTP status code returned by the REST API (0 if connection or client error) |
| `operaton.error.message` | Error message returned by Operaton or the client exception message |

### Process Variables in FlowFiles

Variables in FlowFile content are either:

- **Simple JSON** (default): `{"name": "Ada", "count": 2, "order": {"id": 1}}`.
  Fetch records the original variable types in the `operaton.externalTask.variableTypes` attribute, and Complete / BPMN Error restore them when the value still fits (so `Long`, `Short`, `Date`, `Xml` and serialized `Object` variables survive a round trip). `File` variables are never written back. Other values are typed by their JSON value: strings, booleans, integers (`Integer`/`Long`), decimals (`Double`) and nulls map to the matching Operaton types; numbers that `Long` or `Double` cannot hold exactly are rejected; objects and arrays map to `Json` variables, so the engine needs the Spin JSON plugin.
- **Operaton Typed JSON**: `{"name": {"type": "String", "value": "Ada"}}`, as used by the REST API directly.

## Development

Start the entire local development stack (Operaton, NiFi, Vault, and auto-preconfiguration):

```sh
devenv up
```

Services started:
- **Operaton BPM**: http://127.0.0.1:8080 (REST API at `/engine-rest`, Webapps at `/operaton/`, credentials: `admin` / `admin`)
- **Apache NiFi**: http://127.0.0.1:8081/nifi/ (Plain HTTP, no authentication)
- **HashiCorp Vault**: http://127.0.0.1:8200 (Web UI enabled, unsealed automatically, token stored in `.devenv/state/env_file`)
- **Preconfiguration**: Automatically sets up Vault KV secrets (`secret/operaton`) and preconfigures NiFi with `StandardOperatonExternalTaskService`, `StandardHashiCorpVaultClientService`, and a sample external task processing flow.

Additional commands:
```sh
devenv shell -- build          # mvn package for all modules, NARs under */*-nar/target
devenv shell -- nifi-deploy    # manually copy newly rebuilt NARs into NiFi's autoload directory
devenv shell -- preconfigure   # rerun preconfiguration on demand
devenv shell -- vault status   # interact with local Vault using vault CLI
```

The `services.nifi` module (`devenv/modules/nifi.nix`) packages the official
NiFi 2.4.0 binary, because nixpkgs only has NiFi 1.x. It runs NiFi over plain
HTTP with no authentication (local development only) and keeps its state in
`.devenv/state/nifi`. Built NARs in `narPaths` are automatically deployed to
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

This repository includes a `.devcontainer.json` configuration based on the devenv devcontainer image (`ghcr.io/cachix/devenv/devcontainer:latest`, currently `v2.4.0`) with Direnv and Java extensions pre-configured.

When launched in Codespaces:
- Ports 8080 (Operaton), 8081 (NiFi), and 8200 (Vault) are forwarded automatically.
- NiFi (`nifi.web.proxy.host`) and Operaton (`forwardHeadersStrategy = "native"`) are configured to support Codespaces' forwarded reverse-proxy URLs out of the box.
- Run `devenv up` inside the terminal to boot the full stack.


