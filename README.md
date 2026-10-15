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

| Processor | Does |
| --- | --- |
| FetchAndLockExternalTasks | Locks tasks of a topic; one FlowFile per task, variables as JSON content, task metadata as `operaton.externalTask.*` attributes |
| CompleteExternalTask | Completes the task, setting variables from FlowFile content |
| HandleExternalTaskFailure | Reports a failure; decrements `operaton.externalTask.retries` (or uses *Default Retries* on the first failure); 0 retries creates an incident |
| HandleExternalTaskBpmnError | Throws a BPMN error with an error code |
| ExtendExternalTaskLock | Extends the lock before long running steps |

The task id is read from `${operaton.externalTask.id}` by default. When a call
fails, the FlowFile goes to `failure` with `operaton.error.status` (HTTP status,
0 if none) and `operaton.error.message`, so a failed *Complete* can be routed
straight to *HandleExternalTaskFailure*.

Variables in FlowFile content are either:

- **Simple JSON** (default): `{"name": "Ada", "count": 2, "order": {"id": 1}}`.
  Fetch records the original variable types in the
  `operaton.externalTask.variableTypes` attribute, and Complete / BPMN Error
  restore them when the value still fits (so `Long`, `Short`, `Date`, `Xml` and
  serialized `Object` variables survive a round trip). `File` variables are
  never written back. Other values are typed by their JSON value: strings,
  booleans, integers (Integer/Long), decimals (Double) and nulls map to the
  matching Operaton types; numbers that Long or Double cannot hold exactly are
  rejected; objects and arrays map to `Json` variables, so the engine needs the
  Spin JSON plugin.
- **Operaton Typed JSON**: `{"name": {"type": "String", "value": "Ada"}}`, as
  used by the REST API.

A minimal flow is
`FetchAndLockExternalTasks → (your processing) → CompleteExternalTask`, with
`CompleteExternalTask.failure → HandleExternalTaskFailure`.

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

