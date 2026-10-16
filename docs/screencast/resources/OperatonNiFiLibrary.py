import time
import base64
import json
import subprocess
import urllib.request
from robot.api.deco import keyword
import screencast.library as sl

class OperatonNiFiLibrary:
    """Helper keywords for Operaton and NiFi interaction during screencast."""

    def _get_page(self):
        return sl._SESSION._turn_page if sl._SESSION._turn_context is not None else sl._SESSION.current_page

    @keyword
    def reset_demo_environment(self):
        """Resets and reseeds Operaton demo instances and NiFi flow."""
        subprocess.run(["python3", "docs/scripts/setup_demo_instance.py", "--reset"], check=True)

    @keyword
    def show_narrative_banner(self, text, tag="OPERATON"):
        """Displays a clean floating narrative banner across active screencast contexts."""
        pages = []
        if sl._SESSION.observer_page:
            pages.append(sl._SESSION.observer_page)
        if sl._SESSION._turn_page and sl._SESSION._turn_page not in pages:
            pages.append(sl._SESSION._turn_page)
        if not pages and sl._SESSION.current_page:
            pages.append(sl._SESSION.current_page)

        for p in pages:
            try:
                p.evaluate("""({text, tag}) => {
                    let el = document.getElementById('screencast-narrative-banner');
                    if (!el) {
                        el = document.createElement('div');
                        el.id = 'screencast-narrative-banner';
                        document.body.appendChild(el);
                    }
                    el.style.cssText = `
                        position: fixed;
                        bottom: 28px;
                        left: 50%;
                        transform: translateX(-50%);
                        background: rgba(15, 23, 42, 0.95);
                        color: #f8fafc;
                        padding: 12px 28px;
                        border-radius: 9999px;
                        box-shadow: 0 14px 35px rgba(0,0,0,0.6);
                        border: 1.5px solid rgba(56, 189, 248, 0.6);
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        font-size: 16px;
                        font-weight: 500;
                        z-index: 9999999;
                        pointer-events: none;
                        display: flex;
                        align-items: center;
                        gap: 14px;
                        max-width: 90vw;
                        box-sizing: border-box;
                        letter-spacing: 0.2px;
                        backdrop-filter: blur(8px);
                        transition: all 0.25s ease-in-out;
                    `;
                    const color = tag.includes('NIFI') ? '#f97316' : '#0284c7';
                    el.innerHTML = `<span style="background: ${color}; color: white; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 13px; letter-spacing: 0.6px; text-transform: uppercase;">${tag}</span> <span style="line-height: 1.35;">${text}</span>`;
                }""", {"text": text, "tag": tag})
            except Exception:
                pass
        page = self._get_page()
        page.wait_for_timeout(500)

    @keyword
    def subtle_cursor_glide(self, duration_sec=1.5):
        """Moves cursor slightly across screen to maintain 25fps video life during pacing pauses."""
        page = self._get_page()
        for i in range(2):
            page.mouse.move(600 + (i * 30), 450 + (i * 15), steps=8)
            page.wait_for_timeout(int(float(duration_sec) * 500))

    @keyword
    def inspect_variable_row(self, name):
        """Smoothly moves mouse to a specific variable in Cockpit table to highlight it."""
        page = self._get_page()
        row = page.locator(f'tr:has-text("{name}")').first
        if row.is_visible():
            box = row.bounding_box()
            if box:
                page.mouse.move(box["x"] + 120, box["y"] + box["height"] / 2, steps=10)
                page.wait_for_timeout(600)
                # Glide to the value column
                page.mouse.move(box["x"] + 320, box["y"] + box["height"] / 2, steps=8)
                page.wait_for_timeout(600)

    @keyword
    def highlight_bpmn_activity(self, activity_id):
        """Highlights a BPMN diagram activity and smoothly hovers over it."""
        page = self._get_page()
        elem = page.locator(f'.djs-element[data-element-id="{activity_id}"]').first
        if elem.is_visible():
            box = elem.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=10)
                elem.click(force=True)
                page.wait_for_timeout(600)

    @keyword
    def trace_bpmn_flow(self, from_id, to_id):
        """Smoothly moves mouse along path between two activities."""
        page = self._get_page()
        el1 = page.locator(f'.djs-element[data-element-id="{from_id}"]').first
        el2 = page.locator(f'.djs-element[data-element-id="{to_id}"]').first
        if el1.is_visible() and el2.is_visible():
            b1 = el1.bounding_box()
            b2 = el2.bounding_box()
            if b1 and b2:
                page.mouse.move(b1["x"] + b1["width"] / 2, b1["y"] + b1["height"] / 2, steps=8)
                page.mouse.move(b2["x"] + b2["width"] / 2, b2["y"] + b2["height"] / 2, steps=14)
                page.wait_for_timeout(500)

    @keyword
    def pan_nifi_canvas(self, dx=350, dy=0):
        """Pans the NiFi canvas by dragging the canvas background."""
        page = self._get_page()
        page.mouse.move(800, 500)
        page.mouse.down()
        page.mouse.move(800 + int(dx), 500 + int(dy), steps=12)
        page.mouse.up()
        page.wait_for_timeout(600)

    @keyword
    def inspect_nifi_queue(self, src_proc_name):
        """Hovers over the connection queue downstream of a processor."""
        page = self._get_page()
        proc_text = page.locator(f'text.processor-name:text-is("{src_proc_name}")').first
        if proc_text.is_visible():
            box = proc_text.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] + 45, steps=10)
                page.wait_for_timeout(700)

    @keyword
    def highlight_canvas_processor(self, name):
        """Smoothly moves mouse to a processor on the NiFi canvas and clicks to select it."""
        page = self._get_page()
        proc_text = page.locator(f'text.processor-name:text-is("{name}")').first
        if proc_text.is_visible():
            box = proc_text.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=10)
                proc_text.click(force=True)
                page.wait_for_timeout(600)

    @keyword
    def configure_operaton_local_storage(self):
        """Sets minimal-history-plugin options in localStorage before loading instance."""
        page = self._get_page()
        cfg = {
            "autoRefresh": True,
            "showHistoricBadges": False,
            "showSequenceFlow": True,
            "showHeatmap": False,
            "showInstanceHeatmap": False,
            "leftPaneSize": None,
            "topPaneSize": None,
            "maxResults": 1000
        }
        page.evaluate("(cfg) => localStorage.setItem('minimal-history-plugin', JSON.stringify(cfg))", cfg)

    @keyword
    def trigger_cockpit_refresh(self):
        """Refreshes the Cockpit observer view to immediately update token badges and sequence flows."""
        obs = sl._SESSION.observer_page or self._get_page()
        if obs:
            btn = obs.locator('button.toggle-auto-refresh-button, a[ng-click*="reload"], button[ng-click*="reload"]').first
            if btn.is_visible():
                box = btn.bounding_box()
                if box:
                    obs.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=6)
                btn.click()
            else:
                obs.reload()
                obs.wait_for_selector('.diagram-holder')
            obs.wait_for_timeout(800)

    @keyword
    def start_nifi_canvas_processor(self, name):
        """Selects processor by name and clicks the Start button in operation-control."""
        page = self._get_page()
        proc_text = page.locator(f'text.processor-name:text-is("{name}")').first
        proc_text.wait_for(state="visible", timeout=10000)
        box = proc_text.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        proc_text.click(force=True)
        page.wait_for_timeout(400)
        start_btn = page.locator('operation-control button:has(i.fa-play)').first
        start_btn.wait_for(state="visible", timeout=5000)
        s_box = start_btn.bounding_box()
        if s_box:
            page.mouse.move(s_box["x"] + s_box["width"] / 2, s_box["y"] + s_box["height"] / 2, steps=6)
        start_btn.click(force=True)
        page.wait_for_timeout(700)

    @keyword
    def stop_nifi_canvas_processor(self, name):
        """Selects processor by name and clicks the Stop button in operation-control."""
        page = self._get_page()
        proc_text = page.locator(f'text.processor-name:text-is("{name}")').first
        proc_text.wait_for(state="visible", timeout=10000)
        box = proc_text.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        proc_text.click(force=True)
        page.wait_for_timeout(400)
        stop_btn = page.locator('operation-control button:has(i.fa-stop)').first
        stop_btn.wait_for(state="visible", timeout=5000)
        s_box = stop_btn.bounding_box()
        if s_box:
            page.mouse.move(s_box["x"] + s_box["width"] / 2, s_box["y"] + s_box["height"] / 2, steps=6)
        stop_btn.click(force=True)
        page.wait_for_timeout(700)

    @keyword
    def get_operaton_instance_id_by_key(self, business_key):
        """Fetches the process instance ID for a given business key via REST."""
        auth = base64.b64encode(b"admin:admin").decode("ascii")
        req = urllib.request.Request(
            f"http://127.0.0.1:8080/engine-rest/process-instance?businessKey={business_key}",
            headers={"Authorization": f"Basic {auth}"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if not data:
                raise RuntimeError(f"No instance found for business key {business_key}")
            return data[0]["id"]

    @keyword
    def get_operaton_history_instance_id_by_key(self, business_key):
        """Fetches the latest historical process instance ID for a given business key via REST."""
        auth = base64.b64encode(b"admin:admin").decode("ascii")
        req = urllib.request.Request(
            f"http://127.0.0.1:8080/engine-rest/history/process-instance?processInstanceBusinessKey={business_key}&sortBy=startTime&sortOrder=desc",
            headers={"Authorization": f"Basic {auth}"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if not data:
                raise RuntimeError(f"No history instance found for business key {business_key}")
            return data[0]["id"]
