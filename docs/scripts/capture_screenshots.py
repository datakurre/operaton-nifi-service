#!/usr/bin/env python3
"""
Automated screenshot generator for Operaton NiFi External Task Worker.

Captures screenshots of Apache NiFi (3-column Order Fulfillment & Fraud Compliance
flow, Controller Services, and Vault-backed Parameter Context), Operaton BPM
(Cockpit Dashboard, Process Definition Diagram with live tokens/incidents, and
Tasklist with escalated Manual Order Review task), and HashiCorp Vault UI.

Usage:
    playwright-python docs/scripts/capture_screenshots.py
    playwright-python docs/scripts/capture_screenshots.py --record-video
"""

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request

from playwright.sync_api import sync_playwright

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_NIFI_URL = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081/nifi")
DEFAULT_NIFI_API = os.environ.get("NIFI_API", "http://127.0.0.1:8081/nifi-api")
DEFAULT_OPERATON_URL = os.environ.get("OPERATON_ADDR", "http://127.0.0.1:8080/operaton")
DEFAULT_OPERATON_REST = os.environ.get("OPERATON_REST_URL", "http://127.0.0.1:8080/engine-rest")
DEFAULT_VAULT_URL = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200/ui")
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "images")
VIEWPORT = {"width": 1920, "height": 1080}


def is_service_reachable(url: str, timeout: float = 3.0) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ScreenshotScript/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status < 500
    except urllib.error.HTTPError as e:
        return e.code < 500
    except Exception:
        return False


def api_get(url: str, basic_auth: str = None):
    headers = {"Accept": "application/json"}
    if basic_auth:
        token = base64.b64encode(basic_auth.encode("ascii")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_vault_token() -> str:
    if os.environ.get("VAULT_TOKEN"):
        return os.environ["VAULT_TOKEN"]
    env_file = os.path.join(ROOT_DIR, ".devenv", "state", "env_file")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("export VAULT_TOKEN="):
                    return line.strip().split("=", 1)[1]
    return ""


def capture_nifi(page, nifi_url: str, output_dir: str):
    print(f"Navigating to NiFi: {nifi_url}...")
    try:
        page.goto(nifi_url, wait_until="networkidle", timeout=30000)
    except Exception:
        page.goto(nifi_url, wait_until="load", timeout=30000)

    page.wait_for_timeout(3500)

    # Collapse Navigation and Operation drawers so the canvas has full width, then fit
    try:
        page.evaluate("""() => {
            const nav = document.querySelector('#navigation-control button, .nav-resizer button, button:has(.fa-minus-square-o)');
            const drawers = document.querySelectorAll('button[title="Hide navigation"], button[title="Hide operation"], .dock-panel-resizer');
            drawers.forEach(d => d.click());
            const navPanel = document.querySelector('#navigation-control, .navigation-control');
            if (navPanel) navPanel.style.display = 'none';
            const opPanel = document.querySelector('#operation-control, .operation-control');
            if (opPanel) opPanel.style.display = 'none';
        }""")
        page.wait_for_timeout(500)
        fit_btn = page.locator('button[title="Fit"], button:has(.fa-expand)').first
        if fit_btn.is_visible():
            fit_btn.click()
            page.wait_for_timeout(1000)
    except Exception as e:
        print(f"Notice adjusting NiFi canvas viewport: {e}")

    canvas_shot = os.path.join(output_dir, "01-nifi-flow-canvas.png")
    page.screenshot(path=canvas_shot, full_page=False)
    print(f"  Captured: {canvas_shot}")

    # Navigate directly to Root Process Group Controller Services
    try:
        nifi_api = nifi_url.rstrip("/").replace("/nifi", "") + "/nifi-api"
        root_info = api_get(f"{nifi_api}/flow/process-groups/root")
        root_id = root_info["processGroupFlow"]["breadcrumb"]["breadcrumb"]["id"]
        cs_url = f"{nifi_url}/#/process-groups/{root_id}/controller-services"
        print(f"Navigating to NiFi Controller Services: {cs_url}...")
        page.goto(cs_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(2500)
        services_shot = os.path.join(output_dir, "02-nifi-controller-services.png")
        page.screenshot(path=services_shot)
        print(f"  Captured: {services_shot}")
    except Exception as e:
        print(f"Notice capturing NiFi controller services view: {e}")

    # Navigate to Parameter Contexts view showing Vault-synchronized 'operaton' context
    try:
        pc_url = f"{nifi_url}/#/parameter-contexts"
        print(f"Navigating to NiFi Parameter Contexts: {pc_url}...")
        page.goto(pc_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(2500)
        pc_shot = os.path.join(output_dir, "03-nifi-parameter-context.png")
        page.screenshot(path=pc_shot)
        print(f"  Captured: {pc_shot}")
    except Exception as e:
        print(f"Notice capturing NiFi parameter contexts view: {e}")


def capture_operaton(page, operaton_url: str, output_dir: str):
    cockpit_login_url = f"{operaton_url}/app/cockpit/default/#/login"
    print(f"Navigating to Operaton Cockpit: {cockpit_login_url}...")
    try:
        page.goto(cockpit_login_url, wait_until="networkidle", timeout=30000)
    except Exception:
        page.goto(cockpit_login_url, wait_until="load", timeout=30000)

    page.wait_for_timeout(2000)

    try:
        user_input = page.locator('input[name="username"], input[ng-model="username"]').first
        pass_input = page.locator('input[name="password"], input[ng-model="password"]').first
        submit_btn = page.locator(
            'button[type="submit"], button:has-text("Log in"), button:has-text("Sign in"), button:has-text("Login")'
        ).first

        if user_input.is_visible() and pass_input.is_visible():
            user_input.fill("admin")
            pass_input.fill("admin")
            submit_btn.click()
            page.wait_for_timeout(3500)

            cockpit_shot = os.path.join(output_dir, "04-operaton-cockpit-dashboard.png")
            page.screenshot(path=cockpit_shot)
            print(f"  Captured: {cockpit_shot}")
    except Exception as e:
        print(f"Notice logging into Operaton Cockpit: {e}")

    # Navigate to the deployed 'demo-process' Process Definition runtime diagram view
    try:
        operaton_rest = operaton_url.rstrip("/").replace("/operaton", "") + "/engine-rest"
        defs = api_get(f"{operaton_rest}/process-definition?key=demo-process&latestVersion=true", "admin:admin")
        if defs:
            proc_def_id = defs[0]["id"]
            diagram_url = f"{operaton_url}/app/cockpit/default/#/process-definition/{proc_def_id}/runtime"
            print(f"Navigating to Operaton Process Definition diagram: {diagram_url}...")
            page.goto(diagram_url, wait_until="load", timeout=20000)
            page.wait_for_timeout(3500)
            diagram_shot = os.path.join(output_dir, "05-operaton-process-diagram.png")
            page.screenshot(path=diagram_shot)
            print(f"  Captured: {diagram_shot}")
    except Exception as e:
        print(f"Notice capturing Operaton process diagram: {e}")

    # Operaton Tasklist with 'Manual Order Review' task selected and variables loaded
    try:
        tasklist_url = f"{operaton_url}/app/tasklist/default/"
        print(f"Navigating to Operaton Tasklist: {tasklist_url}...")
        page.goto(tasklist_url, wait_until="load", timeout=20000)
        page.wait_for_timeout(3500)
        task_item = page.get_by_text("Manual Order Review").first
        try:
            task_item.wait_for(state="visible", timeout=6000)
            task_item.click()
            page.wait_for_timeout(1500)
            load_vars = page.get_by_text("Load Variables").first
            if load_vars.is_visible():
                load_vars.click()
                page.wait_for_timeout(1500)
        except Exception:
            pass
        tasklist_shot = os.path.join(output_dir, "06-operaton-tasklist.png")
        page.screenshot(path=tasklist_shot)
        print(f"  Captured: {tasklist_shot}")
    except Exception as e:
        print(f"Notice navigating to Tasklist: {e}")


def capture_vault(page, vault_url: str, output_dir: str):
    base_ui_url = vault_url.rstrip("/")
    if not base_ui_url.endswith("/ui"):
        base_ui_url += "/ui"
    print(f"Navigating to Vault: {base_ui_url}...")
    try:
        page.goto(f"{base_ui_url}/vault/auth", wait_until="networkidle", timeout=15000)
        page.wait_for_timeout(2000)
        vault_token = get_vault_token()
        if vault_token:
            token_input = page.locator('input[name="token"]').or_(page.locator('input[type="password"]')).first
            if token_input.is_visible():
                token_input.fill(vault_token)
                sign_in_btn = page.locator('button[type="submit"]').first
                if sign_in_btn.is_visible():
                    sign_in_btn.click()
                    page.wait_for_timeout(2500)
                    secret_link = page.locator('a:has-text("secret/")').first
                    if secret_link.is_visible():
                        secret_link.click()
                        page.wait_for_timeout(2000)
                        operaton_entry = page.get_by_text("operaton").first
                        if operaton_entry.is_visible():
                            operaton_entry.click()
                            page.wait_for_timeout(2000)
        vault_shot = os.path.join(output_dir, "07-vault-ui.png")
        page.screenshot(path=vault_shot)
        print(f"  Captured: {vault_shot}")
    except Exception as e:
        print(f"Notice capturing Vault UI: {e}")


def main():
    parser = argparse.ArgumentParser(description="Capture screenshots of Operaton NiFi stack")
    parser.add_argument("--nifi-url", default=DEFAULT_NIFI_URL, help="URL for Apache NiFi")
    parser.add_argument("--operaton-url", default=DEFAULT_OPERATON_URL, help="Base URL for Operaton Webapps")
    parser.add_argument("--vault-url", default=DEFAULT_VAULT_URL, help="URL for HashiCorp Vault UI")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for screenshots")
    parser.add_argument("--record-video", action="store_true", help="Record video of session as well")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    nifi_available = is_service_reachable(args.nifi_url)
    operaton_available = is_service_reachable(args.operaton_url)
    vault_available = is_service_reachable(args.vault_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        context_kwargs = {"viewport": VIEWPORT}
        if args.record_video:
            video_dir = os.path.join(os.path.dirname(args.output_dir), "screencast", "recordings")
            os.makedirs(video_dir, exist_ok=True)
            context_kwargs["record_video_dir"] = video_dir
            context_kwargs["record_video_size"] = VIEWPORT
            print(f"Video recording enabled -> {video_dir}")

        context = browser.new_context(**context_kwargs)
        page = context.new_page()

        page.on("console", lambda msg: print(f"  [browser console] {msg.type}: {msg.text}"))
        page.on("pageerror", lambda exc: print(f"  [browser pageerror] {exc}"))

        if nifi_available:
            capture_nifi(page, args.nifi_url, args.output_dir)
        else:
            print(f"Skipping NiFi capture ({args.nifi_url} not reachable).")

        if operaton_available:
            capture_operaton(page, args.operaton_url, args.output_dir)
        else:
            print(f"Skipping Operaton capture ({args.operaton_url} not reachable).")

        if vault_available:
            capture_vault(page, args.vault_url, args.output_dir)
        else:
            print(f"Skipping Vault capture ({args.vault_url} not reachable).")

        context.close()
        browser.close()

    print("\nScreenshot process completed!")
    if os.path.exists(args.output_dir):
        files = sorted(os.listdir(args.output_dir))
        print(f"Screenshots in {args.output_dir}: {files if files else '(none captured)'}")


if __name__ == "__main__":
    main()
