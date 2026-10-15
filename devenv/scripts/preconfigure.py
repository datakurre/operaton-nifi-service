#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.request
import urllib.error

def request(url, data=None, headers=None, method=None):
    if headers is None:
        headers = {}
    if data is not None and isinstance(data, (dict, list)):
        data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
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

def configure_vault(vault_addr, state_dir):
    env_file = os.path.join(state_dir, "env_file")
    vault_token = None
    for _ in range(30):
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
                    "url": "http://localhost:8080/engine-rest",
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

def configure_nifi(nifi_addr, vault_token, operaton_url="http://localhost:8080/engine-rest"):
    print("Configuring NiFi components...")
    root_flow = request(f"{nifi_addr}/nifi-api/flow/process-groups/root")
    root_group = root_flow["processGroupFlow"]["breadcrumb"]["breadcrumb"]
    root_id = root_group["id"]

    # Check existing controller services
    services_flow = request(f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}/controller-services")
    existing_services = {
        s["component"]["name"]: s for s in services_flow.get("controllerServices", [])
    }

    operaton_service_id = None
    if "StandardOperatonExternalTaskService" in existing_services:
        operaton_service_id = existing_services["StandardOperatonExternalTaskService"]["id"]
        print("StandardOperatonExternalTaskService already exists.")
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
                        "properties": {
                            "Operaton REST URL": operaton_url,
                            "Worker ID": "nifi-worker",
                            "Username": "admin",
                            "Password": "admin",
                        },
                    },
                },
                method="POST",
            )
            operaton_service_id = res["id"]
            service_rev = res["revision"]["version"]
            request(
                f"{nifi_addr}/nifi-api/controller-services/{operaton_service_id}/run-status",
                data={"revision": {"version": service_rev}, "state": "ENABLED"},
                method="PUT",
            )
            print("Created and enabled StandardOperatonExternalTaskService.")
        except Exception as e:
            print(f"Error creating StandardOperatonExternalTaskService: {e}")

    # StandardHashiCorpVaultClientService
    if "StandardHashiCorpVaultClientService" in existing_services:
        print("StandardHashiCorpVaultClientService already exists.")
    else:
        try:
            vault_props = {
                "configuration-strategy": "Direct Properties",
                "vault.uri": "http://127.0.0.1:8200",
                "vault.authentication": "TOKEN",
            }
            if vault_token:
                vault_props["vault.token"] = vault_token
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
            service_rev = res["revision"]["version"]
            request(
                f"{nifi_addr}/nifi-api/controller-services/{vault_service_id}/run-status",
                data={"revision": {"version": service_rev}, "state": "ENABLED"},
                method="PUT",
            )
            print("Created and enabled StandardHashiCorpVaultClientService.")
        except Exception as e:
            print(f"Notice creating StandardHashiCorpVaultClientService: {e}")

    # Check processors in root group
    flow_content = request(f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}")
    processors = flow_content["processGroupFlow"]["flow"]["processors"]
    existing_proc_names = {p["component"]["name"] for p in processors}

    if "FetchAndLockExternalTasks" not in existing_proc_names and operaton_service_id:
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

    print("Preconfiguration complete!")

def main():
    state_dir = os.environ.get("DEVENV_STATE", "/workspace/operaton-nifi-service/.devenv/state")
    vault_addr = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
    nifi_addr = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081")

    wait_for_vault(vault_addr)
    vault_token = configure_vault(vault_addr, state_dir)
    wait_for_nifi(nifi_addr)
    configure_nifi(nifi_addr, vault_token)

if __name__ == "__main__":
    main()
