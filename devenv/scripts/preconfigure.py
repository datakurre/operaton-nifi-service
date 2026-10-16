#!/usr/bin/env python3
import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid


def request(url, data=None, headers=None, method=None):
    if headers is None:
        headers = {}
    else:
        headers = dict(headers)
    if data is not None and isinstance(data, (dict, list)):
        data = json.dumps(data).encode("utf-8")
        headers.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8")
            if content:
                try:
                    return json.loads(content)
                except Exception:
                    return content
            return None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8")
        try:
            err_json = json.loads(err_body)
            raise RuntimeError(f"HTTP {e.code} on {url}: {err_json}")
        except RuntimeError:
            raise
        except Exception:
            raise RuntimeError(f"HTTP {e.code} on {url}: {err_body}")


def wait_for_vault(vault_addr):
    print("Waiting for Vault to be unsealed...")
    while True:
        try:
            status = request(f"{vault_addr}/v1/sys/seal-status")
            if status and not status.get("sealed", True):
                print("Vault is unsealed.")
                break
        except Exception:
            pass
        time.sleep(1)


def configure_vault(vault_addr, state_dir, operaton_url="http://localhost:8080/engine-rest"):
    env_file = os.path.join(state_dir, "env_file")
    vault_token = os.environ.get("VAULT_TOKEN")
    for _ in range(30):
        if vault_token:
            break
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    if line.startswith("export VAULT_TOKEN="):
                        vault_token = line.strip().split("=", 1)[1]
                        break
        if vault_token:
            break
        time.sleep(1)

    if not vault_token:
        print("Warning: VAULT_TOKEN not found in env_file; skipping Vault secret setup.")
        return None

    headers = {"X-Vault-Token": vault_token}
    print("Configuring Vault secrets...")
    # Enable KV-v2 secrets engine at secret/ if not already mounted
    try:
        mounts = request(f"{vault_addr}/v1/sys/mounts", headers=headers)
        if mounts and "secret/" not in mounts:
            request(
                f"{vault_addr}/v1/sys/mounts/secret",
                data={"type": "kv", "options": {"version": "2"}},
                headers=headers,
                method="POST",
            )
            print("Enabled KV secrets engine at secret/")
    except Exception as e:
        print(f"Notice enabling secret/ mount: {e}")

    # Put initial Operaton secrets
    try:
        request(
            f"{vault_addr}/v1/secret/data/operaton",
            data={
                "data": {
                    "url": operaton_url,
                    "worker_id": "nifi-worker",
                    "username": "admin",
                    "password": "admin",
                }
            },
            headers=headers,
            method="POST",
        )
        print("Stored initial Operaton secrets in secret/operaton.")
    except Exception as e:
        print(f"Notice storing secret: {e}")

    return vault_token


def wait_for_operaton(operaton_url):
    print("Waiting for Operaton REST API to be ready...")
    auth = base64.b64encode(b"admin:admin").decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}
    while True:
        try:
            ver = request(f"{operaton_url}/version", headers=headers)
            if ver and "version" in ver:
                print(f"Operaton {ver['version']} is ready.")
                break
        except Exception:
            pass
        time.sleep(2)


def configure_operaton(operaton_url, devenv_root):
    print("Configuring Operaton demo deployment and process instance...")
    auth = base64.b64encode(b"admin:admin").decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}

    bpmn_path = os.path.join(devenv_root, "deployment", "demo-process.bpmn")
    if os.path.exists(bpmn_path):
        try:
            defs = request(f"{operaton_url}/process-definition?key=demo-process", headers=headers)
            if not defs:
                boundary = f"----OperatonFormBoundary{uuid.uuid4().hex}"
                with open(bpmn_path, "rb") as f:
                    bpmn_bytes = f.read()
                body = (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="deployment-name"\r\n\r\n'
                    f"demo-deployment\r\n"
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="deploy-changed-only"\r\n\r\n'
                    f"true\r\n"
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="data"; filename="demo-process.bpmn"\r\n'
                    f"Content-Type: application/octet-stream\r\n\r\n"
                ).encode("utf-8") + bpmn_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
                deploy_headers = {
                    "Authorization": f"Basic {auth}",
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                }
                request(f"{operaton_url}/deployment/create", data=body, headers=deploy_headers, method="POST")
                print("Deployed demo-process.bpmn to Operaton.")
            else:
                print("Process definition 'demo-process' is already deployed.")
        except Exception as e:
            print(f"Notice deploying demo-process.bpmn: {e}")

    try:
        tasks = request(f"{operaton_url}/external-task?topicName=demo", headers=headers)
        if not tasks:
            res = request(
                f"{operaton_url}/process-definition/key/demo-process/start",
                data={
                    "businessKey": "DEMO-001",
                    "variables": {
                        "orderId": {"value": "ORD-98765", "type": "String"},
                        "amount": {"value": 149.99, "type": "Double"},
                    },
                },
                headers=headers,
                method="POST",
            )
            print(f"Started demo-process instance: {res.get('id')}")
        else:
            print(f"External task on topic 'demo' already waiting ({len(tasks)} active).")
    except Exception as e:
        print(f"Notice starting demo-process instance: {e}")


def wait_for_nifi(nifi_addr):
    print("Waiting for NiFi to be ready...")
    while True:
        try:
            diag = request(f"{nifi_addr}/nifi-api/system-diagnostics")
            if diag and "systemDiagnostics" in diag:
                print("NiFi is ready.")
                break
        except Exception:
            pass
        time.sleep(2)


def ensure_service_enabled(nifi_addr, service_id):
    for _ in range(15):
        svc = request(f"{nifi_addr}/nifi-api/controller-services/{service_id}")
        state = svc["component"]["state"]
        val_status = svc["component"].get("validationStatus", "VALID")
        if state == "ENABLED":
            return True
        if state == "DISABLED" and val_status == "VALID":
            request(
                f"{nifi_addr}/nifi-api/controller-services/{service_id}/run-status",
                data={"revision": svc["revision"], "state": "ENABLED"},
                method="PUT",
            )
        time.sleep(1)
    return False


def configure_vault_parameter_context(nifi_addr, vault_addr, vault_token, root_id):
    if not vault_token:
        return False

    vault_props = {
        "configuration-strategy": "direct-properties",
        "vault.uri": vault_addr,
        "vault.authentication": "TOKEN",
        "vault.token": vault_token,
    }

    # 1. Ensure controller-level StandardHashiCorpVaultClientService exists and is enabled
    ctrl_services = request(f"{nifi_addr}/nifi-api/flow/controller/controller-services")
    existing_ctrl_services = {
        s["component"]["name"]: s for s in ctrl_services.get("controllerServices", [])
    }

    if "StandardHashiCorpVaultClientService" in existing_ctrl_services:
        ctrl_vault = existing_ctrl_services["StandardHashiCorpVaultClientService"]
        ctrl_vault_id = ctrl_vault["id"]
        if ctrl_vault["component"]["state"] == "DISABLED":
            request(
                f"{nifi_addr}/nifi-api/controller-services/{ctrl_vault_id}",
                data={
                    "revision": ctrl_vault["revision"],
                    "component": {
                        "id": ctrl_vault_id,
                        "name": "StandardHashiCorpVaultClientService",
                        "properties": vault_props,
                    },
                },
                method="PUT",
            )
    else:
        res = request(
            f"{nifi_addr}/nifi-api/controller/controller-services",
            data={
                "revision": {"version": 0},
                "component": {
                    "type": "org.apache.nifi.vault.hashicorp.StandardHashiCorpVaultClientService",
                    "name": "StandardHashiCorpVaultClientService",
                    "bundle": {
                        "group": "org.apache.nifi",
                        "artifact": "nifi-hashicorp-vault-nar",
                        "version": "2.4.0",
                    },
                    "properties": vault_props,
                },
            },
            method="POST",
        )
        ctrl_vault_id = res["id"]
        print("Created controller-level StandardHashiCorpVaultClientService.")

    ensure_service_enabled(nifi_addr, ctrl_vault_id)

    # 2. Ensure HashiCorpVaultParameterProvider exists
    providers_flow = request(f"{nifi_addr}/nifi-api/flow/parameter-providers")
    existing_providers = {
        p["component"]["name"]: p for p in providers_flow.get("parameterProviders", [])
    }

    provider_props = {
        "vault-client-service": ctrl_vault_id,
        "kv-path": "secret",
        "kv-version": "KV_2",
        "secret-name-pattern": ".*",
    }

    if "HashiCorpVaultParameterProvider" in existing_providers:
        provider_id = existing_providers["HashiCorpVaultParameterProvider"]["id"]
    else:
        res = request(
            f"{nifi_addr}/nifi-api/controller/parameter-providers",
            data={
                "revision": {"version": 0},
                "component": {
                    "type": "org.apache.nifi.vault.hashicorp.HashiCorpVaultParameterProvider",
                    "name": "HashiCorpVaultParameterProvider",
                    "bundle": {
                        "group": "org.apache.nifi",
                        "artifact": "nifi-hashicorp-vault-nar",
                        "version": "2.4.0",
                    },
                    "properties": provider_props,
                },
            },
            method="POST",
        )
        provider_id = res["id"]
        print("Created HashiCorpVaultParameterProvider.")

    # 3. Check if Parameter Context 'operaton' already exists with required parameters
    contexts = request(f"{nifi_addr}/nifi-api/flow/parameter-contexts")
    operaton_ctx = next(
        (c for c in contexts.get("parameterContexts", []) if c["component"]["name"] == "operaton"),
        None,
    )
    existing_param_names = set()
    if operaton_ctx:
        existing_param_names = {
            p["parameter"]["name"]
            for p in operaton_ctx["component"].get("parameters", [])
            if "parameter" in p
        }

    if not {"url", "worker_id", "username", "password"}.issubset(existing_param_names):
        # Fetch parameters from Vault and apply to Parameter Context 'operaton'
        provider = request(f"{nifi_addr}/nifi-api/parameter-providers/{provider_id}")
        provider = request(
            f"{nifi_addr}/nifi-api/parameter-providers/{provider_id}/parameters/fetch-requests",
            data={"id": provider_id, "revision": provider["revision"]},
            method="POST",
        )
        apply_req = request(
            f"{nifi_addr}/nifi-api/parameter-providers/{provider_id}/apply-parameters-requests",
            data={
                "id": provider_id,
                "revision": provider["revision"],
                "parameterGroupConfigurations": [
                    {
                        "groupName": "operaton",
                        "parameterContextName": "operaton",
                        "synchronized": True,
                        "parameterSensitivities": {
                            "password": "SENSITIVE",
                            "url": "NON_SENSITIVE",
                            "username": "NON_SENSITIVE",
                            "worker_id": "NON_SENSITIVE",
                        },
                    }
                ],
            },
            method="POST",
        )
        apply_req_id = apply_req["request"]["requestId"]
        for _ in range(15):
            status = request(
                f"{nifi_addr}/nifi-api/parameter-providers/{provider_id}/apply-parameters-requests/{apply_req_id}"
            )
            if status["request"].get("complete"):
                break
            time.sleep(0.5)
        request(
            f"{nifi_addr}/nifi-api/parameter-providers/{provider_id}/apply-parameters-requests/{apply_req_id}",
            method="DELETE",
        )
        print("Synchronized Vault secret/operaton to NiFi Parameter Context 'operaton'.")
        contexts = request(f"{nifi_addr}/nifi-api/flow/parameter-contexts")
        operaton_ctx = next(
            (c for c in contexts.get("parameterContexts", []) if c["component"]["name"] == "operaton"),
            None,
        )

    # 4. Bind Parameter Context 'operaton' to root Process Group
    if operaton_ctx:
        pg = request(f"{nifi_addr}/nifi-api/process-groups/{root_id}")
        current_ctx = pg["component"].get("parameterContext", {})
        if current_ctx.get("id") != operaton_ctx["id"]:
            request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}",
                data={
                    "revision": pg["revision"],
                    "component": {
                        "id": root_id,
                        "parameterContext": {"id": operaton_ctx["id"]},
                    },
                },
                method="PUT",
            )
            print("Bound Parameter Context 'operaton' to root Process Group.")
        return True
    return False


def configure_nifi(nifi_addr, vault_addr, vault_token, operaton_url="http://localhost:8080/engine-rest"):
    print("Configuring NiFi components...")
    root_flow = request(f"{nifi_addr}/nifi-api/flow/process-groups/root")
    root_group = root_flow["processGroupFlow"]["breadcrumb"]["breadcrumb"]
    root_id = root_group["id"]

    vault_synced = False
    try:
        vault_synced = configure_vault_parameter_context(nifi_addr, vault_addr, vault_token, root_id)
    except Exception as e:
        print(f"Notice configuring Vault Parameter Provider: {e}")

    # Check existing controller services in root process group
    services_flow = request(f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}/controller-services")
    existing_services = {
        s["component"]["name"]: s for s in services_flow.get("controllerServices", [])
    }

    operaton_props = (
        {
            "Operaton REST URL": "#{url}",
            "Worker ID": "#{worker_id}",
            "Username": "#{username}",
            "Password": "#{password}",
        }
        if vault_synced
        else {
            "Operaton REST URL": operaton_url,
            "Worker ID": "nifi-worker",
            "Username": "admin",
            "Password": "admin",
        }
    )

    operaton_service_id = None
    if "StandardOperatonExternalTaskService" in existing_services:
        svc = existing_services["StandardOperatonExternalTaskService"]
        operaton_service_id = svc["id"]
        if svc["component"]["state"] == "DISABLED":
            request(
                f"{nifi_addr}/nifi-api/controller-services/{operaton_service_id}",
                data={
                    "revision": svc["revision"],
                    "component": {
                        "id": operaton_service_id,
                        "name": "StandardOperatonExternalTaskService",
                        "properties": operaton_props,
                    },
                },
                method="PUT",
            )
        ensure_service_enabled(nifi_addr, operaton_service_id)
        print("StandardOperatonExternalTaskService is configured and enabled.")
    else:
        try:
            res = request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}/controller-services",
                data={
                    "revision": {"version": 0},
                    "component": {
                        "type": "org.operaton.bpm.extension.nifi.controller.StandardOperatonExternalTaskService",
                        "name": "StandardOperatonExternalTaskService",
                        "bundle": {
                            "group": "org.operaton.bpm.extension.nifi",
                            "artifact": "operaton-nifi-service-nar",
                            "version": "1.0-SNAPSHOT",
                        },
                        "properties": operaton_props,
                    },
                },
                method="POST",
            )
            operaton_service_id = res["id"]
            ensure_service_enabled(nifi_addr, operaton_service_id)
            print("Created and enabled StandardOperatonExternalTaskService.")
        except Exception as e:
            print(f"Error creating StandardOperatonExternalTaskService: {e}")

    # Process-group-level StandardHashiCorpVaultClientService
    vault_props = {
        "configuration-strategy": "direct-properties",
        "vault.uri": vault_addr,
        "vault.authentication": "TOKEN",
    }
    if vault_token:
        vault_props["vault.token"] = vault_token

    if "StandardHashiCorpVaultClientService" in existing_services:
        svc = existing_services["StandardHashiCorpVaultClientService"]
        vault_service_id = svc["id"]
        if svc["component"]["state"] == "DISABLED":
            request(
                f"{nifi_addr}/nifi-api/controller-services/{vault_service_id}",
                data={
                    "revision": svc["revision"],
                    "component": {
                        "id": vault_service_id,
                        "name": "StandardHashiCorpVaultClientService",
                        "properties": vault_props,
                    },
                },
                method="PUT",
            )
        ensure_service_enabled(nifi_addr, vault_service_id)
        print("Process-group StandardHashiCorpVaultClientService is configured and enabled.")
    else:
        try:
            res = request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}/controller-services",
                data={
                    "revision": {"version": 0},
                    "component": {
                        "type": "org.apache.nifi.vault.hashicorp.StandardHashiCorpVaultClientService",
                        "name": "StandardHashiCorpVaultClientService",
                        "bundle": {
                            "group": "org.apache.nifi",
                            "artifact": "nifi-hashicorp-vault-nar",
                            "version": "2.4.0",
                        },
                        "properties": vault_props,
                    },
                },
                method="POST",
            )
            vault_service_id = res["id"]
            ensure_service_enabled(nifi_addr, vault_service_id)
            print("Created and enabled process-group StandardHashiCorpVaultClientService.")
        except Exception as e:
            print(f"Notice creating StandardHashiCorpVaultClientService: {e}")

    # Check processors in root group
    flow_content = request(f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}")
    processors = flow_content["processGroupFlow"]["flow"]["processors"]
    existing_procs = {p["component"]["name"]: p for p in processors}

    if "FetchAndLockExternalTasks" not in existing_procs and operaton_service_id:
        try:
            fetch_res = request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}/processors",
                data={
                    "revision": {"version": 0},
                    "component": {
                        "type": "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
                        "name": "FetchAndLockExternalTasks",
                        "position": {"x": 300, "y": 100},
                        "bundle": {
                            "group": "org.operaton.bpm.extension.nifi",
                            "artifact": "operaton-nifi-processors-nar",
                            "version": "1.0-SNAPSHOT",
                        },
                        "config": {
                            "properties": {
                                "External Task Service": operaton_service_id,
                                "Topic Name": "demo",
                            },
                        },
                    },
                },
                method="POST",
            )
            fetch_id = fetch_res["id"]
            print("Created FetchAndLockExternalTasks processor.")

            complete_res = request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}/processors",
                data={
                    "revision": {"version": 0},
                    "component": {
                        "type": "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
                        "name": "CompleteExternalTask",
                        "position": {"x": 300, "y": 350},
                        "bundle": {
                            "group": "org.operaton.bpm.extension.nifi",
                            "artifact": "operaton-nifi-processors-nar",
                            "version": "1.0-SNAPSHOT",
                        },
                        "config": {
                            "properties": {
                                "External Task Service": operaton_service_id,
                            },
                            "autoTerminatedRelationships": ["success", "failure"],
                        },
                    },
                },
                method="POST",
            )
            complete_id = complete_res["id"]
            print("Created CompleteExternalTask processor.")

            # Connect FetchAndLockExternalTasks -> CompleteExternalTask
            request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}/connections",
                data={
                    "revision": {"version": 0},
                    "component": {
                        "source": {"id": fetch_id, "type": "PROCESSOR", "groupId": root_id},
                        "destination": {"id": complete_id, "type": "PROCESSOR", "groupId": root_id},
                        "selectedRelationships": ["success"],
                    },
                },
                method="POST",
            )
            print("Connected FetchAndLockExternalTasks -> CompleteExternalTask.")
        except Exception as e:
            print(f"Notice setting up demo flow: {e}")

    if "HandleExternalTaskFailure" not in existing_procs and operaton_service_id:
        try:
            failure_res = request(
                f"{nifi_addr}/nifi-api/process-groups/{root_id}/processors",
                data={
                    "revision": {"version": 0},
                    "component": {
                        "type": "org.operaton.bpm.extension.nifi.processor.HandleExternalTaskFailure",
                        "name": "HandleExternalTaskFailure",
                        "position": {"x": 750, "y": 350},
                        "bundle": {
                            "group": "org.operaton.bpm.extension.nifi",
                            "artifact": "operaton-nifi-processors-nar",
                            "version": "1.0-SNAPSHOT",
                        },
                        "config": {
                            "properties": {
                                "External Task Service": operaton_service_id,
                            },
                            "autoTerminatedRelationships": ["success", "failure"],
                        },
                    },
                },
                method="POST",
            )
            failure_id = failure_res["id"]
            print("Created HandleExternalTaskFailure processor.")

            # Connect CompleteExternalTask (failure) -> HandleExternalTaskFailure
            flow_content = request(f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}")
            procs = {p["component"]["name"]: p for p in flow_content["processGroupFlow"]["flow"]["processors"]}
            if "CompleteExternalTask" in procs:
                comp_proc = procs["CompleteExternalTask"]
                comp_id = comp_proc["id"]
                request(
                    f"{nifi_addr}/nifi-api/ processors/{comp_id}".replace(" ", ""),
                    data={
                        "revision": comp_proc["revision"],
                        "component": {
                            "id": comp_id,
                            "config": {
                                "autoTerminatedRelationships": ["success"],
                            },
                        },
                    },
                    method="PUT",
                )
                request(
                    f"{nifi_addr}/nifi-api/process-groups/{root_id}/connections",
                    data={
                        "revision": {"version": 0},
                        "component": {
                            "source": {"id": comp_id, "type": "PROCESSOR", "groupId": root_id},
                            "destination": {"id": failure_id, "type": "PROCESSOR", "groupId": root_id},
                            "selectedRelationships": ["failure"],
                        },
                    },
                    method="POST",
                )
                print("Connected CompleteExternalTask (failure) -> HandleExternalTaskFailure.")
        except Exception as e:
            print(f"Notice setting up HandleExternalTaskFailure: {e}")

    print("Preconfiguration complete!")


def main():
    devenv_root = os.environ.get("DEVENV_ROOT", "/workspace/operaton-nifi-service")
    state_dir = os.environ.get("DEVENV_STATE", os.path.join(devenv_root, ".devenv", "state"))
    vault_addr = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
    nifi_addr = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081")
    operaton_url = os.environ.get("OPERATON_REST_URL", "http://localhost:8080/engine-rest")

    wait_for_vault(vault_addr)
    vault_token = configure_vault(vault_addr, state_dir, operaton_url)
    wait_for_operaton(operaton_url)
    configure_operaton(operaton_url, devenv_root)
    wait_for_nifi(nifi_addr)
    configure_nifi(nifi_addr, vault_addr, vault_token, operaton_url)


if __name__ == "__main__":
    main()
