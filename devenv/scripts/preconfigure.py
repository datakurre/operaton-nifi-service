#!/usr/bin/env python3
import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid

OPERATON_USER = os.environ.get("OPERATON_ADMIN_USER_ID", os.environ.get("OPERATON_USERNAME", "demo"))
OPERATON_PASS = os.environ.get("OPERATON_ADMIN_USER_PASSWORD", os.environ.get("OPERATON_PASSWORD", "demo"))


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

    try:
        request(
            f"{vault_addr}/v1/secret/data/operaton",
            data={
                "data": {
                    "url": operaton_url,
                    "worker_id": "nifi-worker",
                    "username": OPERATON_USER,
                    "password": OPERATON_PASS,
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
    auth = base64.b64encode(f"{OPERATON_USER}:{OPERATON_PASS}".encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}
    while True:
        try:
            ver = request(f"{operaton_url}/version", headers=headers)
            if ver and "version" in ver:
                print(f"Operaton {ver['version']} is ready.")
                break
        except Exception:
            try:
                fallback_auth = base64.b64encode(b"admin:admin").decode("ascii")
                ver = request(f"{operaton_url}/version", headers={"Authorization": f"Basic {fallback_auth}"})
                if ver and "version" in ver:
                    print(f"Operaton {ver['version']} is ready.")
                    break
            except Exception:
                pass
        time.sleep(2)


def _fetch_and_lock_by_business_key(operaton_url, headers, topic, business_key, worker_id="demo-seeder"):
    tasks = request(
        f"{operaton_url}/external-task/fetchAndLock",
        data={
            "workerId": worker_id,
            "maxTasks": 1,
            "topics": [
                {
                    "topicName": topic,
                    "lockDuration": 60000,
                    "businessKey": business_key,
                }
            ],
        },
        headers=headers,
        method="POST",
    )
    if tasks:
        return tasks[0]["id"]
    return None


def configure_operaton(operaton_url, devenv_root):
    print("Configuring Operaton demo deployment and multi-stage process instances...")
    auth = base64.b64encode(f"{OPERATON_USER}:{OPERATON_PASS}".encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}

    bpmn_path = os.path.join(devenv_root, "deployment", "demo-process.bpmn")
    if os.path.exists(bpmn_path):
        try:
            defs = request(
                f"{operaton_url}/process-definition?key=demo-process&versionTag=2.0",
                headers=headers,
            )
            if not defs:
                old_instances = request(
                    f"{operaton_url}/process-instance?processDefinitionKey=demo-process",
                    headers=headers,
                ) or []
                for inst in old_instances:
                    try:
                        request(
                            f"{operaton_url}/process-instance/{inst['id']}?skipCustomListeners=true",
                            headers=headers,
                            method="DELETE",
                        )
                    except Exception:
                        pass

                boundary = f"----OperatonFormBoundary{uuid.uuid4().hex}"
                with open(bpmn_path, "rb") as f:
                    bpmn_bytes = f.read()
                body = (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="deployment-name"\r\n\r\n'
                    f"order-fulfillment-demo\r\n"
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
                print("Deployed demo-process.bpmn (versionTag 2.0) to Operaton.")
            else:
                print("Process definition 'demo-process' (versionTag 2.0) is already deployed.")
        except Exception as e:
            print(f"Notice deploying demo-process.bpmn: {e}")

    # Seed realistic order instances across stages if not already present
    try:
        active_instances = request(
            f"{operaton_url}/process-instance?processDefinitionKey=demo-process",
            headers=headers,
        ) or []
        existing_keys = {inst.get("businessKey") for inst in active_instances}

        # 1. Completed order (ORD-1000) for History view
        hist_completed = request(
            f"{operaton_url}/history/process-instance?processDefinitionKey=demo-process&processInstanceBusinessKey=ORD-1000",
            headers=headers,
        ) or []
        if not hist_completed:
            request(
                f"{operaton_url}/process-definition/key/demo-process/start",
                data={
                    "businessKey": "ORD-1000",
                    "variables": {
                        "orderId": {"value": "ORD-1000", "type": "String"},
                        "customer": {"value": "Edsger Dijkstra", "type": "String"},
                        "amount": {"value": 89.00, "type": "Double"},
                        "currency": {"value": "EUR", "type": "String"},
                    },
                },
                headers=headers,
                method="POST",
            )
            for topic, extra_vars in [
                ("demo", {"validated": {"value": True, "type": "Boolean"}}),
                ("order-payment", {"paymentId": {"value": "PAY-88201", "type": "String"}}),
                ("order-shipment", {"trackingNumber": {"value": "TRK-992014", "type": "String"}}),
            ]:
                tid = _fetch_and_lock_by_business_key(operaton_url, headers, topic, "ORD-1000")
                if tid:
                    request(
                        f"{operaton_url}/external-task/{tid}/complete",
                        data={"workerId": "demo-seeder", "variables": extra_vars},
                        headers=headers,
                        method="POST",
                    )
            print("Seeded completed order instance ORD-1000.")

        # 2. Order waiting at Stage 1: Validate & Enrich Order (topic 'demo')
        if "ORD-1001" not in existing_keys:
            request(
                f"{operaton_url}/process-definition/key/demo-process/start",
                data={
                    "businessKey": "ORD-1001",
                    "variables": {
                        "orderId": {"value": "ORD-1001", "type": "String"},
                        "customer": {"value": "Ada Lovelace", "type": "String"},
                        "amount": {"value": 149.99, "type": "Double"},
                        "currency": {"value": "EUR", "type": "String"},
                    },
                },
                headers=headers,
                method="POST",
            )
            print("Seeded active order ORD-1001 at 'Validate & Enrich Order' (topic: demo).")

        # 3. High-value order waiting at Stage 2: Fraud & Compliance Check (topic 'order-fraud-check')
        if "ORD-1002" not in existing_keys:
            request(
                f"{operaton_url}/process-definition/key/demo-process/start",
                data={
                    "businessKey": "ORD-1002",
                    "variables": {
                        "orderId": {"value": "ORD-1002", "type": "String"},
                        "customer": {"value": "Grace Hopper", "type": "String"},
                        "amount": {"value": 2450.00, "type": "Double"},
                        "currency": {"value": "EUR", "type": "String"},
                    },
                },
                headers=headers,
                method="POST",
            )
            tid = _fetch_and_lock_by_business_key(operaton_url, headers, "demo", "ORD-1002")
            if tid:
                request(
                    f"{operaton_url}/external-task/{tid}/complete",
                    data={
                        "workerId": "demo-seeder",
                        "variables": {
                            "validated": {"value": True, "type": "Boolean"},
                            "riskTier": {"value": "HIGH_VALUE", "type": "String"},
                        },
                    },
                    headers=headers,
                    method="POST",
                )
            print("Seeded high-value order ORD-1002 at 'Fraud & Compliance Check' (topic: order-fraud-check).")

        # 4. Flagged order escalated via BPMN Error to Human User Task 'Manual Order Review'
        if "ORD-1003" not in existing_keys:
            request(
                f"{operaton_url}/process-definition/key/demo-process/start",
                data={
                    "businessKey": "ORD-1003",
                    "variables": {
                        "orderId": {"value": "ORD-1003", "type": "String"},
                        "customer": {"value": "Alan Turing", "type": "String"},
                        "amount": {"value": 18500.00, "type": "Double"},
                        "currency": {"value": "EUR", "type": "String"},
                    },
                },
                headers=headers,
                method="POST",
            )
            tid_demo = _fetch_and_lock_by_business_key(operaton_url, headers, "demo", "ORD-1003")
            if tid_demo:
                request(
                    f"{operaton_url}/external-task/{tid_demo}/complete",
                    data={
                        "workerId": "demo-seeder",
                        "variables": {"validated": {"value": True, "type": "Boolean"}},
                    },
                    headers=headers,
                    method="POST",
                )
            tid_fraud = _fetch_and_lock_by_business_key(operaton_url, headers, "order-fraud-check", "ORD-1003")
            if tid_fraud:
                request(
                    f"{operaton_url}/external-task/{tid_fraud}/bpmnError",
                    data={
                        "workerId": "demo-seeder",
                        "errorCode": "FRAUD_DETECTED",
                        "errorMessage": "Order ORD-1003 (18,500.00 EUR) exceeds automated compliance limit of 10,000.00 EUR",
                        "variables": {
                            "fraudReason": {
                                "value": "Exceeds 10,000 EUR automated threshold; requires manual compliance sign-off",
                                "type": "String",
                            },
                            "flaggedBy": {"value": "nifi-worker", "type": "String"},
                        },
                    },
                    headers=headers,
                    method="POST",
                )
            print("Seeded flagged order ORD-1003 escalated via BPMN Error to 'Manual Order Review' User Task.")

        # 5. Order with technical failure & Open Incident at 'Charge Payment & Invoice' (topic 'order-payment')
        if "ORD-1004" not in existing_keys:
            request(
                f"{operaton_url}/process-definition/key/demo-process/start",
                data={
                    "businessKey": "ORD-1004",
                    "variables": {
                        "orderId": {"value": "ORD-1004", "type": "String"},
                        "customer": {"value": "Margaret Hamilton", "type": "String"},
                        "amount": {"value": 680.00, "type": "Double"},
                        "currency": {"value": "EUR", "type": "String"},
                    },
                },
                headers=headers,
                method="POST",
            )
            for topic in ["demo", "order-fraud-check"]:
                tid = _fetch_and_lock_by_business_key(operaton_url, headers, topic, "ORD-1004")
                if tid:
                    request(
                        f"{operaton_url}/external-task/{tid}/complete",
                        data={
                            "workerId": "demo-seeder",
                            "variables": {"validated": {"value": True, "type": "Boolean"}},
                        },
                        headers=headers,
                        method="POST",
                    )
            tid_pay = _fetch_and_lock_by_business_key(operaton_url, headers, "order-payment", "ORD-1004")
            if tid_pay:
                request(
                    f"{operaton_url}/external-task/{tid_pay}/failure",
                    data={
                        "workerId": "demo-seeder",
                        "errorMessage": "Upstream payment gateway HTTP 504 Gateway Timeout (retries exhausted)",
                        "errorDetails": "java.net.http.HttpTimeoutException: request timed out calling https://payments.internal/v1/charge for order ORD-1004",
                        "retries": 0,
                        "retryTimeout": 0,
                    },
                    headers=headers,
                    method="POST",
                )
            print("Seeded order ORD-1004 with technical failure incident at 'Charge Payment & Invoice'.")
    except Exception as e:
        print(f"Notice seeding demo-process instances: {e}")


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


def _create_processor(nifi_addr, root_id, name, proc_type, bundle, x, y, props, auto_term=None):
    config = {"properties": props}
    if auto_term:
        config["autoTerminatedRelationships"] = auto_term
    res = request(
        f"{nifi_addr}/nifi-api/process-groups/{root_id}/processors",
        data={
            "revision": {"version": 0},
            "component": {
                "type": proc_type,
                "name": name,
                "position": {"x": x, "y": y},
                "bundle": bundle,
                "config": config,
            },
        },
        method="POST",
    )
    return res["id"]


def _connect(nifi_addr, root_id, src_id, dst_id, relationships, bends=None):
    comp = {
        "source": {"id": src_id, "type": "PROCESSOR", "groupId": root_id},
        "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": root_id},
        "selectedRelationships": relationships,
    }
    if bends:
        comp["bends"] = bends
    request(
        f"{nifi_addr}/nifi-api/process-groups/{root_id}/connections",
        data={"revision": {"version": 0}, "component": comp},
        method="POST",
    )


def configure_nifi_flow(nifi_addr, root_id, operaton_service_id, force_rebuild=False):
    flow_content = request(f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}")
    processors = flow_content["processGroupFlow"]["flow"]["processors"]
    connections = flow_content["processGroupFlow"]["flow"]["connections"]
    existing_names = {p["component"]["name"] for p in processors}

    # Canvases built before lock extension became automatic still have the removed ExtendExternalTaskLock
    if not force_rebuild and "Extract Order Attributes" in existing_names and "ExtendExternalTaskLock" not in existing_names:
        print("Multi-stage Order Fulfillment NiFi flow is already configured.")
        return

    try:
        request(
            f"{nifi_addr}/nifi-api/flow/process-groups/{root_id}",
            data={"id": root_id, "state": "STOPPED"},
            method="PUT",
        )
        time.sleep(1)
    except Exception:
        pass

    for conn in connections:
        try:
            cid = conn["id"]
            drop = request(f"{nifi_addr}/nifi-api/flowfile-queues/{cid}/drop-requests", data=b"", method="POST")
            if drop and "dropRequest" in drop:
                drop_id = drop["dropRequest"]["id"]
                request(f"{nifi_addr}/nifi-api/flowfile-queues/{cid}/drop-requests/{drop_id}", method="DELETE")
        except Exception:
            pass
        try:
            c_fresh = request(f"{nifi_addr}/nifi-api/connections/{conn['id']}")
            ver = c_fresh["revision"]["version"] if c_fresh else conn["revision"]["version"]
            request(
                f"{nifi_addr}/nifi-api/connections/{conn['id']}?version={ver}",
                method="DELETE",
            )
        except Exception:
            pass
    for proc in processors:
        try:
            p_fresh = request(f"{nifi_addr}/nifi-api/processors/{proc['id']}")
            ver = p_fresh["revision"]["version"] if p_fresh else proc["revision"]["version"]
            request(
                f"{nifi_addr}/nifi-api/processors/{proc['id']}?version={ver}",
                method="DELETE",
            )
        except Exception:
            pass

    operaton_bundle = {
        "group": "org.operaton.bpm.extension.nifi",
        "artifact": "operaton-nifi-processors-nar",
        "version": "1.0-SNAPSHOT",
    }
    standard_bundle = {
        "group": "org.apache.nifi",
        "artifact": "nifi-standard-nar",
        "version": "2.4.0",
    }
    update_attr_bundle = {
        "group": "org.apache.nifi",
        "artifact": "nifi-update-attribute-nar",
        "version": "2.4.0",
    }

    # --- Column 1 (x=80): Stage 1 - Order Validation & Enrichment (topic: demo) ---
    fetch_validate = _create_processor(
        nifi_addr,
        root_id,
        "FetchAndLockExternalTasks",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        80,
        40,
        {
            "External Task Service": operaton_service_id,
            "Topic Name": "demo",
        },
    )
    eval_json = _create_processor(
        nifi_addr,
        root_id,
        "Extract Order Attributes",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        80,
        240,
        {
            "Destination": "flowfile-attribute",
            "order.id": "$.orderId",
            "order.amount": "$.amount",
            "order.customer": "$.customer",
        },
        auto_term=["failure", "unmatched"],
    )
    route_validate = _create_processor(
        nifi_addr,
        root_id,
        "Validate Order Rules",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        standard_bundle,
        80,
        440,
        {
            "Routing Strategy": "Route to Property name",
            "invalid": "${order.amount:toNumber():le(0):or(${order.id:isEmpty()})}",
        },
    )
    enrich_order = _create_processor(
        nifi_addr,
        root_id,
        "Enrich Order Metadata",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        update_attr_bundle,
        80,
        640,
        {
            "validation.status": "VALIDATED",
            "validation.worker": "nifi-worker",
        },
    )
    complete_validate = _create_processor(
        nifi_addr,
        root_id,
        "CompleteExternalTask",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        80,
        840,
        {
            "External Task Service": operaton_service_id,
        },
        auto_term=["success"],
    )
    bpmn_validation_error = _create_processor(
        nifi_addr,
        root_id,
        "HandleValidationError",
        "org.operaton.bpm.extension.nifi.processor.HandleExternalTaskBpmnError",
        operaton_bundle,
        80,
        1040,
        {
            "External Task Service": operaton_service_id,
            "Error Code": "VALIDATION_ERROR",
            "Error Message": "Order ${order.id} failed validation rules",
        },
        auto_term=["success"],
    )

    # --- Column 2 (x=580): Stage 2 - High-Value Fraud & Compliance (topic: order-fraud-check) ---
    fetch_fraud = _create_processor(
        nifi_addr,
        root_id,
        "Fetch Fraud Check Tasks",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        580,
        40,
        {
            "External Task Service": operaton_service_id,
            "Topic Name": "order-fraud-check",
        },
    )
    eval_fraud_json = _create_processor(
        nifi_addr,
        root_id,
        "Extract Fraud Check Attributes",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        580,
        240,
        {
            "Destination": "flowfile-attribute",
            "order.id": "$.orderId",
            "order.amount": "$.amount",
            "order.customer": "$.customer",
        },
        auto_term=["failure", "unmatched"],
    )
    route_fraud = _create_processor(
        nifi_addr,
        root_id,
        "Evaluate Fraud Risk",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        standard_bundle,
        580,
        640,
        {
            "Routing Strategy": "Route to Property name",
            "fraud_suspected": "${order.amount:toNumber():gt(10000)}",
        },
    )
    complete_fraud = _create_processor(
        nifi_addr,
        root_id,
        "Complete Fraud Check",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        580,
        840,
        {
            "External Task Service": operaton_service_id,
        },
        auto_term=["success"],
    )
    bpmn_fraud_error = _create_processor(
        nifi_addr,
        root_id,
        "HandleFraudBpmnError",
        "org.operaton.bpm.extension.nifi.processor.HandleExternalTaskBpmnError",
        operaton_bundle,
        580,
        1040,
        {
            "External Task Service": operaton_service_id,
            "Error Code": "FRAUD_DETECTED",
            "Error Message": "Order ${order.id} escalated to Manual Order Review",
        },
        auto_term=["success"],
    )

    # --- Column 3 (x=1080 & x=1520): Stage 3 - Payment, Shipment & Incident Handling ---
    fetch_payment = _create_processor(
        nifi_addr,
        root_id,
        "Fetch Payment Tasks",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        1080,
        40,
        {
            "External Task Service": operaton_service_id,
            "Topic Name": "order-payment",
        },
    )
    fetch_shipment = _create_processor(
        nifi_addr,
        root_id,
        "Fetch Shipment Tasks",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        1520,
        40,
        {
            "External Task Service": operaton_service_id,
            "Topic Name": "order-shipment",
        },
    )
    complete_fulfillment = _create_processor(
        nifi_addr,
        root_id,
        "Complete Payment & Shipment",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        1300,
        340,
        {
            "External Task Service": operaton_service_id,
        },
        auto_term=["success"],
    )
    handle_failure = _create_processor(
        nifi_addr,
        root_id,
        "HandleExternalTaskFailure",
        "org.operaton.bpm.extension.nifi.processor.HandleExternalTaskFailure",
        operaton_bundle,
        1300,
        740,
        {
            "External Task Service": operaton_service_id,
            "Default Retries": "3",
            "Retry Timeout": "1 min",
        },
        auto_term=["success", "failure"],
    )

    # Connect Column 1
    _connect(nifi_addr, root_id, fetch_validate, eval_json, ["success"])
    _connect(nifi_addr, root_id, eval_json, route_validate, ["matched"])
    _connect(nifi_addr, root_id, route_validate, enrich_order, ["unmatched"])
    _connect(nifi_addr, root_id, enrich_order, complete_validate, ["success"])
    _connect(nifi_addr, root_id, route_validate, bpmn_validation_error, ["invalid"])
    _connect(
        nifi_addr,
        root_id,
        bpmn_validation_error,
        handle_failure,
        ["failure"],
        bends=[{"x": 480, "y": 1090}],
    )
    _connect(
        nifi_addr,
        root_id,
        complete_validate,
        handle_failure,
        ["failure"],
        bends=[{"x": 760, "y": 890}],
    )

    # Connect Column 2
    _connect(nifi_addr, root_id, fetch_fraud, eval_fraud_json, ["success"])
    _connect(nifi_addr, root_id, eval_fraud_json, route_fraud, ["matched"])
    _connect(nifi_addr, root_id, route_fraud, complete_fraud, ["unmatched"])
    _connect(nifi_addr, root_id, route_fraud, bpmn_fraud_error, ["fraud_suspected"])
    _connect(nifi_addr, root_id, complete_fraud, handle_failure, ["failure"])
    _connect(
        nifi_addr,
        root_id,
        bpmn_fraud_error,
        handle_failure,
        ["failure"],
        bends=[{"x": 980, "y": 1090}],
    )

    # Connect Column 3 & Failure Handling
    _connect(nifi_addr, root_id, fetch_payment, complete_fulfillment, ["success"])
    _connect(nifi_addr, root_id, fetch_shipment, complete_fulfillment, ["success"])
    _connect(nifi_addr, root_id, complete_fulfillment, handle_failure, ["failure"])

    print("Created 3-stage Order Fulfillment & Fraud Compliance flow in NiFi.")


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
            "Username": OPERATON_USER,
            "Password": OPERATON_PASS,
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

    if operaton_service_id:
        try:
            configure_nifi_flow(nifi_addr, root_id, operaton_service_id)
        except Exception as e:
            print(f"Notice configuring NiFi flow: {e}")

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
