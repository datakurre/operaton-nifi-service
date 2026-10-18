#!/usr/bin/env python3
"""
Captures high-resolution documentation screenshots for the Operaton NiFi tutorial:
1. nifi-travel-canvas.png: 4-column NiFi Travel Planner pipeline
2. config-controller-service.png: StandardOperatonExternalTaskService configuration
3. config-parameter-context.png: Vault-backed Parameter Context dialog
4. config-processor-fetch.png: FetchAndLockExternalTasks processor properties
5. config-processor-complete.png: CompleteExternalTask processor properties
6. operaton-cockpit-model.png: Operaton Cockpit runtime diagram with live tokens
7. operaton-tasklist-form.png: Operaton Tasklist form with pre-populated variables
8. mailpit-itinerary-email.png: Mailpit Web UI showing rendered adventure guide email
"""

import base64
import json
import os
import time
import urllib.request
from playwright.sync_api import sync_playwright

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NIFI_URL = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081/nifi")
NIFI_API = os.environ.get("NIFI_API", "http://127.0.0.1:8081/nifi-api")
OPERATON_URL = os.environ.get("OPERATON_ADDR", "http://127.0.0.1:8080/operaton")
OPERATON_REST = os.environ.get("OPERATON_REST_URL", "http://127.0.0.1:8080/engine-rest")
MAILPIT_URL = os.environ.get("MAILPIT_URL", "http://127.0.0.1:8025")
OPERATON_USER = os.environ.get("OPERATON_ADMIN_USER_ID", "demo")
OPERATON_PASS = os.environ.get("OPERATON_ADMIN_USER_PASSWORD", "demo")
OUTPUT_DIR = os.path.join(ROOT_DIR, "docs", "images")
VIEWPORT = {"width": 1920, "height": 1080}


def api_get(url: str, basic_auth: str = None):
    headers = {"Accept": "application/json"}
    if basic_auth:
        token = base64.b64encode(basic_auth.encode("ascii")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_travel_planner_group_id():
    resp = api_get(f"{NIFI_API}/flow/process-groups/root")
    for group in resp["processGroupFlow"]["flow"]["processGroups"]:
        if "Travel Planner" in group["component"]["name"]:
            return group["id"]
    return resp["processGroupFlow"]["breadcrumb"]["breadcrumb"]["id"]


def start_trip(preset="tampere"):
    auth = base64.b64encode(f"{OPERATON_USER}:{OPERATON_PASS}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    body = {
        "variables": {
            "presetDestination": {"value": preset, "type": "String"},
            "travelerName": {"value": "Ada Lovelace", "type": "String"},
            "travelerEmail": {"value": "ada.lovelace@example.test", "type": "String"},
        },
        "businessKey": f"TRIP-{preset.upper()}-LIVE",
    }
    req = urllib.request.Request(
        f"{OPERATON_REST}/process-definition/key/finnish-travel-planner/start",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    inst_id, task_id = None, None
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            res = json.loads(resp.read().decode())
            inst_id = res["id"]
            print(f"Started live trip instance {inst_id} ({preset}). Waiting for User Task...")
    except Exception as e:
        print(f"Notice starting trip: {e}")
        return None, None

    for _ in range(30):
        time.sleep(1)
        req = urllib.request.Request(
            f"{OPERATON_REST}/task?processInstanceId={inst_id}&taskDefinitionKey=UserTask_CurateTrip",
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                tasks = json.loads(resp.read().decode())
                if tasks:
                    task_id = tasks[0]["id"]
                    print(f"User Task 'UserTask_CurateTrip' active: {task_id}")
                    break
        except Exception:
            pass
    return inst_id, task_id


def submit_curation_task(task_id):
    if not task_id:
        return
    auth = base64.b64encode(f"{OPERATON_USER}:{OPERATON_PASS}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    body = {
        "variables": {
            "seasonVibe": {"value": "ruska", "type": "String"},
            "stayStyle": {"value": "mökki", "type": "String"},
            "diningStyle": {"value": "campfire", "type": "String"},
            "dietaryNotes": {"value": "Fish & local berry desserts", "type": "String"},
            "specialNotes": {"value": "Campfire coffee on lake shore.", "type": "String"},
            "confirmTrip": {"value": True, "type": "Boolean"},
        }
    }
    req = urllib.request.Request(
        f"{OPERATON_REST}/task/{task_id}/complete",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            pass
        print("User task completed. Waiting for Mailpit email delivery...")
        time.sleep(3)
    except Exception as e:
        print(f"Notice completing user task: {e}")


def capture_all():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1.25)
        page = context.new_page()

        # ---------------------------------------------------------
        # 1. NiFi Canvas: 4-Column Flow
        # ---------------------------------------------------------
        group_id = get_travel_planner_group_id()
        canvas_url = f"{NIFI_URL}/#/process-groups/{group_id}"
        print(f"1. Capturing NiFi Canvas: {canvas_url}...")
        page.goto(canvas_url, wait_until="load", timeout=30000)
        page.wait_for_timeout(3500)

        try:
            fit_btn = page.locator('button[title="Fit"], button:has(.fa-expand)').first
            if fit_btn.is_visible():
                fit_btn.click()
                page.wait_for_timeout(1000)
            page.evaluate("""() => {
                const drawers = document.querySelectorAll('button[title="Hide navigation"], button[title="Hide operation"], .dock-panel-resizer');
                drawers.forEach(d => d.click());
                const navPanel = document.querySelector('#navigation-control, .navigation-control');
                if (navPanel) navPanel.style.display = 'none';
                const opPanel = document.querySelector('#operation-control, .operation-control');
                if (opPanel) opPanel.style.display = 'none';
            }""")
            page.wait_for_timeout(1000)
        except Exception as e:
            print(f"Notice hiding drawers: {e}")

        shot_canvas = os.path.join(OUTPUT_DIR, "nifi-travel-canvas.png")
        page.screenshot(path=shot_canvas)
        print(f"   Saved {shot_canvas}")

        # ---------------------------------------------------------
        # 2. NiFi Controller Service Configuration
        # ---------------------------------------------------------
        cs_url = f"{NIFI_URL}/#/process-groups/{group_id}/controller-services"
        print(f"2. Capturing Controller Services: {cs_url}...")
        page.goto(cs_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(3000)

        # Open the configure dialog for StandardOperatonExternalTaskService
        try:
            row = page.locator('tr:has-text("StandardOperatonExternalTaskService")').first
            if row.is_visible():
                action_btn = row.locator('button').last
                if action_btn.is_visible():
                    action_btn.click()
                    page.wait_for_timeout(500)
                    edit_opt = page.locator('button:has-text("Edit"), [role="menuitem"]:has-text("Edit"), .context-menu-item:has-text("Edit")').first
                    if edit_opt.is_visible():
                        edit_opt.click()
                    else:
                        row.dblclick(force=True)
                else:
                    row.dblclick(force=True)
                page.wait_for_timeout(1500)
                # Click Properties tab
                prop_tab = page.locator('[role="tab"]:has-text("Properties"), .mdc-tab:has-text("Properties"), .tab:has-text("Properties"), button:has-text("Properties"), li:has-text("Properties")').first
                if prop_tab.is_visible():
                    prop_tab.click()
                page.wait_for_timeout(1500)
        except Exception as e:
            print(f"Notice opening CS dialog: {e}")

        shot_cs = os.path.join(OUTPUT_DIR, "config-controller-service.png")
        page.screenshot(path=shot_cs)
        print(f"   Saved {shot_cs}")

        # Close dialog if open
        try:
            cancel_btn = page.locator('button:has-text("Cancel"), button:has-text("Close"), .dialog-close').first
            if cancel_btn.is_visible():
                cancel_btn.click()
                page.wait_for_timeout(500)
        except Exception:
            pass

        # ---------------------------------------------------------
        # 3. NiFi Parameter Context Configuration
        # ---------------------------------------------------------
        pc_url = f"{NIFI_URL}/#/parameter-contexts"
        print(f"3. Capturing Parameter Contexts: {pc_url}...")
        page.goto(pc_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(3000)

        try:
            row = page.locator('tr:has-text("operaton")').first
            if row.is_visible():
                action_btn = row.locator('button').last
                if action_btn.is_visible():
                    action_btn.click()
                    page.wait_for_timeout(500)
                    edit_opt = page.locator('button:has-text("Edit"), [role="menuitem"]:has-text("Edit"), .context-menu-item:has-text("Edit")').first
                    if edit_opt.is_visible():
                        edit_opt.click()
                    else:
                        row.dblclick(force=True)
                else:
                    row.dblclick(force=True)
                page.wait_for_timeout(1500)
                prop_tab = page.locator('[role="tab"]:has-text("Parameters"), .mdc-tab:has-text("Parameters"), .tab:has-text("Parameters"), button:has-text("Parameters")').first
                if prop_tab.is_visible():
                    prop_tab.click()
                page.wait_for_timeout(1500)
        except Exception as e:
            print(f"Notice opening PC dialog: {e}")

        shot_pc = os.path.join(OUTPUT_DIR, "config-parameter-context.png")
        page.screenshot(path=shot_pc)
        print(f"   Saved {shot_pc}")

        try:
            close_btn = page.locator('button:has-text("Cancel"), button:has-text("Close"), .dialog-close').first
            if close_btn.is_visible():
                close_btn.click()
                page.wait_for_timeout(500)
        except Exception:
            pass

        # ---------------------------------------------------------
        # 4. Processor Configuration: FetchAndLockExternalTasks
        # ---------------------------------------------------------
        print(f"4. Capturing Fetch Processor Config...")
        page.goto(canvas_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(3000)
        try:
            page.evaluate("""() => {
                const gc = document.querySelector('graph-controls');
                if (gc) gc.style.display = 'none';
            }""")
            fetch_proc = page.locator('text="Fetch Destination Task"').first
            if fetch_proc.is_visible():
                fetch_proc.dblclick(force=True)
                page.wait_for_timeout(1500)
                prop_tab = page.locator('[role="tab"]:has-text("Properties"), .mdc-tab:has-text("Properties"), button:has-text("Properties")').first
                if prop_tab.is_visible():
                    prop_tab.click(force=True)
                page.wait_for_timeout(1500)
        except Exception as e:
            print(f"Notice opening Fetch proc dialog: {e}")

        shot_fetch = os.path.join(OUTPUT_DIR, "config-processor-fetch.png")
        page.screenshot(path=shot_fetch)
        print(f"   Saved {shot_fetch}")

        try:
            cancel_btn = page.locator('button:has-text("Cancel"), button:has-text("Close")').first
            if cancel_btn.is_visible():
                cancel_btn.click()
                page.wait_for_timeout(500)
        except Exception:
            pass

        # ---------------------------------------------------------
        # 5. Processor Configuration: CompleteExternalTask / BPMN Error
        # ---------------------------------------------------------
        print(f"5. Capturing Complete / BPMN Error Processor Config...")
        try:
            page.evaluate("""() => {
                const gc = document.querySelector('graph-controls');
                if (gc) gc.style.display = 'none';
            }""")
            comp_proc = page.locator('text="Complete Destination Lookup"').first
            if comp_proc.is_visible():
                comp_proc.dblclick(force=True)
                page.wait_for_timeout(1500)
                prop_tab = page.locator('[role="tab"]:has-text("Properties"), .mdc-tab:has-text("Properties"), button:has-text("Properties")').first
                if prop_tab.is_visible():
                    prop_tab.click(force=True)
                page.wait_for_timeout(1500)
        except Exception as e:
            print(f"Notice opening Complete proc dialog: {e}")

        shot_comp = os.path.join(OUTPUT_DIR, "config-processor-complete.png")
        page.screenshot(path=shot_comp)
        print(f"   Saved {shot_comp}")

        try:
            close_btn = page.locator('button:has-text("Cancel"), button:has-text("Close")').first
            if close_btn.is_visible():
                close_btn.click()
                page.wait_for_timeout(500)
        except Exception:
            pass

        # Launch fresh trip instance to demonstrate active token & pre-populated tasklist form
        inst_id, task_id = start_trip("tampere")

        # ---------------------------------------------------------
        # 6. Operaton Cockpit: BPMN Diagram with live tokens
        # ---------------------------------------------------------
        cockpit_url = f"{OPERATON_URL}/app/cockpit/default/#/login"
        print(f"6. Capturing Operaton Cockpit: {cockpit_url}...")
        page.goto(cockpit_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(2000)
        try:
            user_input = page.locator('input[name="username"], input[ng-model="username"]').first
            pass_input = page.locator('input[name="password"], input[ng-model="password"]').first
            submit_btn = page.locator('button[type="submit"], button:has-text("Log in"), button:has-text("Sign in")').first
            if user_input.is_visible() and pass_input.is_visible():
                user_input.fill(OPERATON_USER)
                pass_input.fill(OPERATON_PASS)
                submit_btn.click()
                page.wait_for_timeout(3000)
        except Exception as e:
            print(f"Notice logging into Cockpit: {e}")

        try:
            defs = api_get(f"{OPERATON_REST}/process-definition?key=finnish-travel-planner&latestVersion=true", f"{OPERATON_USER}:{OPERATON_PASS}")
            if defs:
                proc_def_id = defs[0]["id"]
                diagram_url = f"{OPERATON_URL}/app/cockpit/default/#/process-definition/{proc_def_id}/runtime"
                print(f"   Navigating to Process Diagram: {diagram_url}...")
                page.goto(diagram_url, wait_until="load", timeout=20000)
                page.wait_for_timeout(3500)
        except Exception as e:
            print(f"Notice navigating to Cockpit diagram: {e}")

        shot_cockpit = os.path.join(OUTPUT_DIR, "operaton-cockpit-model.png")
        page.screenshot(path=shot_cockpit)
        print(f"   Saved {shot_cockpit}")

        # ---------------------------------------------------------
        # 7. Operaton Tasklist: Curation User Task
        # ---------------------------------------------------------
        tasklist_url = f"{OPERATON_URL}/app/tasklist/default/#/"
        print(f"7. Capturing Operaton Tasklist: {tasklist_url}...")
        page.goto(tasklist_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(3000)
        try:
            u_in = page.locator('input[name="username"], input[ng-model="username"]').first
            p_in = page.locator('input[name="password"], input[ng-model="password"]').first
            btn = page.locator('button[type="submit"], button:has-text("Log in"), button:has-text("Sign in")').first
            if u_in.is_visible() and p_in.is_visible():
                u_in.fill(OPERATON_USER)
                p_in.fill(OPERATON_PASS)
                btn.click()
                page.wait_for_timeout(3000)
        except Exception:
            pass

        try:
            task_item = page.locator('text="Curate Trip, Sauna & Dining Plan"').first
            if task_item.is_visible():
                task_item.click()
                page.wait_for_timeout(2000)
                load_vars = page.get_by_text("Load Variables").first
                if load_vars.is_visible():
                    load_vars.click()
                    page.wait_for_timeout(1500)
        except Exception as e:
            print(f"Notice clicking Tasklist task: {e}")

        shot_tasklist = os.path.join(OUTPUT_DIR, "operaton-tasklist-form.png")
        page.screenshot(path=shot_tasklist)
        print(f"   Saved {shot_tasklist}")

        # Complete curation task to trigger itinerary compilation and Mailpit delivery
        if task_id:
            submit_curation_task(task_id)

        # ---------------------------------------------------------
        # 8. Mailpit Web UI: Rendered Itinerary Email
        # ---------------------------------------------------------
        print(f"8. Capturing Mailpit Web UI: {MAILPIT_URL}...")
        page.goto(MAILPIT_URL, wait_until="load", timeout=20000)
        page.wait_for_timeout(3000)
        try:
            mail_item = page.get_by_text("Finnish Adventure Guide").first
            if mail_item.is_visible():
                mail_item.click()
                page.wait_for_timeout(2000)
                preview = page.locator('#message-view, .message-body, iframe, #preview-html').first
                if preview.is_visible():
                    page.wait_for_timeout(1000)
        except Exception as e:
            print(f"Notice clicking Mailpit email: {e}")

        shot_mailpit = os.path.join(OUTPUT_DIR, "mailpit-itinerary-email.png")
        page.screenshot(path=shot_mailpit)
        print(f"   Saved {shot_mailpit}")

        browser.close()
        print("All screenshots captured successfully!")


if __name__ == "__main__":
    capture_all()
