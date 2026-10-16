#!/usr/bin/env python3
"""
Ensures demo-process.bpmn is deployed to Operaton and starts a process instance
waiting on external task topic "demo".
"""
import json
import urllib.request
import urllib.error

OPERATON_REST = "http://127.0.0.1:8080/engine-rest"
AUTH_HEADER = "Basic YWRtaW46YWRtaW4="  # admin:admin

def rest_call(url, data=None, headers=None, method=None):
    if headers is None:
        headers = {}
    headers["Authorization"] = AUTH_HEADER
    if data is not None and isinstance(data, (dict, list)):
        data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        content = resp.read().decode("utf-8")
        return json.loads(content) if content else None

def deploy_bpmn_if_missing(bpmn_path="deployment/demo-process.bpmn"):
    defs = rest_call(f"{OPERATON_REST}/process-definition?key=demo-process")
    if not defs:
        print("Deploying demo-process.bpmn to Operaton...")
        import subprocess
        cmd = [
            "curl", "-s", "-u", "admin:admin",
            "-F", "deployment-name=demo-deployment",
            "-F", f"data=@{bpmn_path}",
            f"{OPERATON_REST}/deployment/create"
        ]
        subprocess.check_call(cmd)
        print("Deployment successful.")
    else:
        print("Process definition 'demo-process' already deployed.")

def start_instance_if_none():
    tasks = rest_call(f"{OPERATON_REST}/external-task?topicName=demo")
    if not tasks:
        print("Starting a new instance of demo-process...")
        payload = {
            "variables": {
                "orderId": {"value": "ORD-98765", "type": "String"},
                "amount": {"value": 149.99, "type": "Double"}
            }
        }
        res = rest_call(f"{OPERATON_REST}/process-definition/key/demo-process/start", data=payload, method="POST")
        print(f"Started instance: {res.get('id')}")
    else:
        print(f"External task for topic 'demo' already waiting ({len(tasks)} task(s)).")

if __name__ == "__main__":
    deploy_bpmn_if_missing()
    start_instance_if_none()
