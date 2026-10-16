#!/usr/bin/env python3
"""
Ensures demo-process.bpmn (v2.0 Order Fulfillment & Fraud Compliance) is deployed
to Operaton and seeds the multi-stage demo order instances (ORD-1000..ORD-1004).

Pass --reset to re-deploy demo-process.bpmn, recreate the 4 active demo instances,
and rebuild the 3-column NiFi flow cleanly.
"""
import argparse
import base64
import os
import sys
import uuid

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from devenv.scripts.preconfigure import (  # noqa: E402
    configure_nifi_flow,
    configure_operaton,
    request,
    wait_for_nifi,
    wait_for_operaton,
)

OPERATON_REST = os.environ.get("OPERATON_REST_URL", "http://127.0.0.1:8080/engine-rest")
NIFI_ADDR = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081")


def reset_operaton_and_nifi():
    auth = base64.b64encode(b"admin:admin").decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}
    instances = (
        request(f"{OPERATON_REST}/process-instance?processDefinitionKey=demo-process", headers=headers)
        or []
    )
    for inst in instances:
        inst_id = inst["id"]
        request(
            f"{OPERATON_REST}/process-instance/{inst_id}?skipCustomListeners=true",
            headers=headers,
            method="DELETE",
        )
    bpmn_path = os.path.join(ROOT_DIR, "deployment", "demo-process.bpmn")
    with open(bpmn_path, "rb") as f:
        bpmn_bytes = f.read()
    boundary = f"----OperatonBoundary{uuid.uuid4().hex}"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="deployment-name"\r\n\r\n'
        f"order-fulfillment-demo\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="data"; filename="demo-process.bpmn"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode("utf-8") + bpmn_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
    request(
        f"{OPERATON_REST}/deployment/create",
        data=body,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )

    wait_for_nifi(NIFI_ADDR)
    root_id = request(f"{NIFI_ADDR}/nifi-api/flow/process-groups/root")["processGroupFlow"]["breadcrumb"][
        "breadcrumb"
    ]["id"]
    svcs = {
        s["component"]["name"]: s["id"]
        for s in request(f"{NIFI_ADDR}/nifi-api/flow/process-groups/{root_id}/controller-services")[
            "controllerServices"
        ]
    }
    if "StandardOperatonExternalTaskService" in svcs:
        configure_nifi_flow(
            NIFI_ADDR, root_id, svcs["StandardOperatonExternalTaskService"], force_rebuild=True
        )


def main():
    parser = argparse.ArgumentParser(description="Setup or reset demo process and instances")
    parser.add_argument("--reset", action="store_true", help="Reset and re-seed Operaton instances and NiFi flow")
    args = parser.parse_args()

    wait_for_operaton(OPERATON_REST)
    if args.reset:
        reset_operaton_and_nifi()
    configure_operaton(OPERATON_REST, ROOT_DIR)


if __name__ == "__main__":
    main()
