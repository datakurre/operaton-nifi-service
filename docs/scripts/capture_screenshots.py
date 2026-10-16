#!/usr/bin/env python3
"""
Automated screenshot generator for Operaton NiFi External Task Worker.

Captures screenshots of Apache NiFi, Operaton BPM (Cockpit/Tasklist),
and HashiCorp Vault when the stack is running.

Usage:
    playwright-python docs/scripts/capture_screenshots.py
    playwright-python docs/scripts/capture_screenshots.py --record-video
"""

import argparse
import os
import sys
import time
import urllib.request
import urllib.error

from playwright.sync_api import sync_playwright

DEFAULT_NIFI_URL = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081/nifi")
DEFAULT_OPERATON_URL = os.environ.get("OPERATON_ADDR", "http://127.0.0.1:8080/operaton")
DEFAULT_VAULT_URL = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200/ui")
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "images")
VIEWPORT = {"width": 1920, "height": 1080}


def is_service_reachable(url: str, timeout: float = 3.0) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ScreenshotScript/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status < 500
    except urllib.error.HTTPError as e:
        # 401/403/302 mean the server is running and responding
        return e.code < 500
    except Exception:
        return False


def wait_for_service(name: str, url: str, max_wait: int = 15) -> bool:
    print(f"Checking if {name} is reachable at {url}...")
    for _ in range(max_wait):
        if is_service_reachable(url):
            print(f"  ✓ {name} is ready.")
            return True
        time.sleep(1)
    print(f"  ✗ {name} is not reachable at {url}.")
    return False


def capture_nifi(page, nifi_url: str, output_dir: str):
    print(f"Navigating to NiFi: {nifi_url}...")
    try:
        page.goto(nifi_url, wait_until="networkidle", timeout=30000)
    except Exception as e:
        print(f"Warning navigating to NiFi networkidle: {e}. Trying load state...")
        page.goto(nifi_url, wait_until="load", timeout=30000)

    # Allow canvas to initialize and render SVG components
    page.wait_for_timeout(3000)

    canvas_shot = os.path.join(output_dir, "01-nifi-flow-canvas.png")
    page.screenshot(path=canvas_shot, full_page=False)
    print(f"  Captured: {canvas_shot}")

    # Try opening controller services list if global menu is accessible
    try:
        # Click on settings / controller services if button exists
        settings_button = page.locator('#global-menu-button, button[title*="Settings"], i.fa-gear, i.fa-cog').first
        if settings_button.is_visible():
            settings_button.click()
            page.wait_for_timeout(1000)
            controller_services_tab = page.locator('text="Controller Services", text="CONTROLLER SERVICES"').first
            if controller_services_tab.is_visible():
                controller_services_tab.click()
                page.wait_for_timeout(1500)
                services_shot = os.path.join(output_dir, "02-nifi-controller-services.png")
                page.screenshot(path=services_shot)
                print(f"  Captured: {services_shot}")
    except Exception as e:
        print(f"Notice capturing NiFi controller services dialog: {e}")


def capture_operaton(page, operaton_url: str, output_dir: str):
    cockpit_login_url = f"{operaton_url}/app/cockpit/default/#/login"
    print(f"Navigating to Operaton Cockpit: {cockpit_login_url}...")
    try:
        page.goto(cockpit_login_url, wait_until="networkidle", timeout=30000)
    except Exception:
        page.goto(cockpit_login_url, wait_until="load", timeout=30000)

    page.wait_for_timeout(2000)
    login_shot = os.path.join(output_dir, "03-operaton-login.png")
    page.screenshot(path=login_shot)
    print(f"  Captured: {login_shot}")

    # Fill login form if present
    try:
        user_input = page.locator('input[name="username"], input[ng-model="username"]').first
        pass_input = page.locator('input[name="password"], input[ng-model="password"]').first
        submit_btn = page.locator('button[type="submit"], button:has-text("Sign in"), button:has-text("Login")').first

        if user_input.is_visible() and pass_input.is_visible():
            user_input.fill("admin")
            pass_input.fill("admin")
            submit_btn.click()
            page.wait_for_timeout(4000)

            cockpit_shot = os.path.join(output_dir, "04-operaton-cockpit-dashboard.png")
            page.screenshot(path=cockpit_shot)
            print(f"  Captured: {cockpit_shot}")
    except Exception as e:
        print(f"Notice logging into Operaton Cockpit: {e}")

    # Operaton Tasklist
    try:
        tasklist_url = f"{operaton_url}/app/tasklist/default/"
        page.goto(tasklist_url, wait_until="load", timeout=15000)
        page.wait_for_timeout(3000)
        tasklist_shot = os.path.join(output_dir, "05-operaton-tasklist.png")
        page.screenshot(path=tasklist_shot)
        print(f"  Captured: {tasklist_shot}")
    except Exception as e:
        print(f"Notice navigating to Tasklist: {e}")


def capture_vault(page, vault_url: str, output_dir: str):
    print(f"Navigating to Vault: {vault_url}...")
    try:
        page.goto(vault_url, wait_until="load", timeout=15000)
        page.wait_for_timeout(2000)
        vault_shot = os.path.join(output_dir, "06-vault-ui.png")
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

    if not (nifi_available or operaton_available or vault_available):
        print("=" * 70)
        print("Note: Services are not currently responding.")
        print("To start the full local development stack, run:")
        print("    devenv up")
        print()
        print("Services expected:")
        print(f"  - NiFi:     {args.nifi_url}")
        print(f"  - Operaton: {args.operaton_url}")
        print(f"  - Vault:    {args.vault_url}")
        print("=" * 70)
        print("Proceeding anyway with reachable services or headless diagnosis...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
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
        files = os.listdir(args.output_dir)
        print(f"Screenshots in {args.output_dir}: {files if files else '(none captured)'}")


if __name__ == "__main__":
    main()
