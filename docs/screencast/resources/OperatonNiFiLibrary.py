import os
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
        subprocess.run(["python3", "docs/scripts/setup_travel_planner.py"], check=True)

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
        proc_text = page.locator(f'text.processor-name:has-text("{name}")').first
        if not proc_text.is_visible():
            page.wait_for_timeout(1000)
        if proc_text.is_visible():
            box = proc_text.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=10)
                proc_text.click(force=True)
    @keyword
    def zoom_to_canvas_processor(self, name, scale=0.85):
        """Smoothly zooms and centers NiFi canvas on a specific processor, then selects it."""
        page = self._get_page()
        page.evaluate("""(data) => {
            const proc = Array.from(document.querySelectorAll("g.processor")).find(g => {
                const nameEl = g.querySelector("text.processor-name");
                return nameEl && nameEl.textContent.includes(data.name);
            });
            if (!proc) return;
            const match = proc.getAttribute("transform").match(/translate\\(([-\\d.]+),\\s*([-\\d.]+)\\)/);
            if (!match) return;
            const px = parseFloat(match[1]);
            const py = parseFloat(match[2]);
            const s = parseFloat(data.scale);
            const cx = px + 176;
            const cy = py + 65;
            const tx = 960 - (cx * s);
            const ty = 500 - (cy * s);
            const canvas = document.querySelector("#canvas");
            canvas.style.transition = "transform 0.4s ease-out";
            canvas.setAttribute("transform", "translate(" + tx + "," + ty + ") scale(" + s + ")");
        }""", {"name": name, "scale": scale})
        page.wait_for_timeout(450)
        proc_text = page.locator(f'text.processor-name:has-text("{name}")').first
        if proc_text.is_visible():
            box = proc_text.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
                proc_text.click(force=True)
                page.wait_for_timeout(600)

    @keyword
    def reset_nifi_canvas_zoom(self):
        """Resets NiFi canvas zoom to show the full pipeline."""
        page = self._get_page()
        page.evaluate("""() => {
            const canvas = document.querySelector("#canvas");
            canvas.style.transition = "transform 0.45s ease-out";
            canvas.setAttribute("transform", "translate(-16.67, 4.17) scale(0.52)");
        }""")
        page.wait_for_timeout(500)

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

    def _make_operaton_request(self, url):
        operaton_user = os.environ.get("OPERATON_ADMIN_USER_ID", "demo")
        operaton_pass = os.environ.get("OPERATON_ADMIN_USER_PASSWORD", "demo")
        auth = base64.b64encode(f"{operaton_user}:{operaton_pass}".encode("ascii")).decode("ascii")
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 401:
                fallback_auth = base64.b64encode(b"admin:admin").decode("ascii")
                req2 = urllib.request.Request(url, headers={"Authorization": f"Basic {fallback_auth}"})
                with urllib.request.urlopen(req2, timeout=10) as resp2:
                    return json.loads(resp2.read().decode())
            raise

    @keyword
    def get_operaton_instance_id_by_key(self, business_key):
        """Fetches the process instance ID for a given business key via REST."""
        data = self._make_operaton_request(
            f"http://127.0.0.1:8080/engine-rest/process-instance?businessKey={business_key}"
        )
        if not data:
            raise RuntimeError(f"No instance found for business key {business_key}")
        return data[0]["id"]

    @keyword
    def get_operaton_history_instance_id_by_key(self, business_key):
        """Fetches the latest historical process instance ID for a given business key via REST."""
        data = self._make_operaton_request(
            f"http://127.0.0.1:8080/engine-rest/history/process-instance?processInstanceBusinessKey={business_key}&sortBy=startTime&sortOrder=desc"
        )
        if not data:
            raise RuntimeError(f"No history instance found for business key {business_key}")
        return data[0]["id"]

    @keyword
    def navigate_to_process_definition_in_cockpit(self, process_key):
        """Navigates to the process definition diagram in Operaton Cockpit by querying its ID."""
        data = self._make_operaton_request(
            f"http://127.0.0.1:8080/engine-rest/process-definition/key/{process_key}"
        )
        def_id = data["id"]
        
        page = self._get_page()
        page.goto(f"http://127.0.0.1:8080/operaton/app/cockpit/default/#/process-definition/{def_id}")
        page.wait_for_selector(".diagram-holder", timeout=10000)
        page.wait_for_timeout(1000)

    @keyword
    def prepare_travel_planner_state(self):
        """Ensures Mailpit, BPMN deployment, and NiFi flow are ready for travel planner."""
        subprocess.run(["python3", "docs/scripts/setup_travel_planner.py"], check=True)

    @keyword
    def get_travel_planner_group_id(self):
        """Fetches process group ID for 'Finnish Travel Planner' in NiFi."""
        req = urllib.request.Request("http://127.0.0.1:8081/nifi-api/flow/process-groups/root")
        with urllib.request.urlopen(req, timeout=10) as resp:
            root = json.loads(resp.read().decode())
            for pg in root["processGroupFlow"]["flow"]["processGroups"]:
                if pg["component"]["name"] == "Finnish Travel Planner":
                    return pg["component"]["id"]
            raise RuntimeError("Finnish Travel Planner process group not found")

    @keyword
    def log_in_to_operaton_tasklist(self, username="admin", password="admin"):
        """Logs into Operaton Tasklist."""
        page = self._get_page()
        page.goto("http://127.0.0.1:8080/operaton/app/tasklist/default/#/login")
        user_input = page.locator('input[ng-model="username"]')
        user_input.wait_for(state="visible", timeout=10000)
        box = user_input.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=6)
        user_input.fill(username)
        page.wait_for_timeout(300)
        pass_input = page.locator('input[ng-model="password"]')
        box = pass_input.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=6)
        pass_input.fill(password)
        page.wait_for_timeout(300)
        btn = page.locator('button[type="submit"]')
        box = btn.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=6)
        btn.click()
        page.wait_for_timeout(1000)

    @keyword
    def start_process_in_tasklist(self, preset="koli"):
        """Opens Start Process modal in Tasklist, selects preset, and starts instance."""
        page = self._get_page()
        # Click "Start process"
        start_btn = page.locator('a:has-text("Start process"), button:has-text("Start process"), [ng-click*="startProcess"]').first
        start_btn.wait_for(state="visible", timeout=10000)
        box = start_btn.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        start_btn.click()
        page.wait_for_timeout(800)

        # Click Finnish Travel & Outdoor Adventure Planner
        proc_link = page.locator('a:has-text("Finnish Travel & Outdoor Adventure Planner")').first
        proc_link.wait_for(state="visible", timeout=10000)
        box = proc_link.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        proc_link.click()
        page.wait_for_timeout(800)

        # Select preset
        preset_select = page.locator('select[cam-variable-name="presetDestination"], select[name="presetDestination"]').first
        if preset_select.is_visible():
            box = preset_select.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
            preset_select.select_option(preset)
            page.wait_for_timeout(600)

        # Click Start in modal footer
        start_instance_btn = page.locator('.modal-footer button:has-text("Start")').first
        box = start_instance_btn.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        start_instance_btn.click()
        page.wait_for_timeout(1500)

    @keyword
    def open_and_claim_curation_task(self):
        """Selects and claims the Curate Trip user task in Tasklist."""
        page = self._get_page()
        task_link = None
        for _ in range(45):
            candidate = page.locator('a:has-text("Curate Trip")').first
            if candidate.is_visible():
                task_link = candidate
                break
            refresh_btn = page.locator('button:has(.glyphicon-repeat), a:has-text("All tasks"), [ng-click*="updateList"]').first
            if refresh_btn.is_visible():
                refresh_btn.click()
            page.wait_for_timeout(1000)

        if not task_link:
            raise RuntimeError("Curate Trip task link not found in Tasklist")

        box = task_link.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        task_link.click()
        page.wait_for_timeout(1000)

        # Claim if available
        claim_btn = page.locator('a:has-text("Claim"), button:has-text("Claim")').first
        if claim_btn.is_visible():
            box = claim_btn.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=6)
            claim_btn.click()
            page.wait_for_timeout(800)

    @keyword
    def fill_curation_choices_and_submit(self):
        """Fills curation form choices (Season, Stay, Dining) and clicks Complete."""
        page = self._get_page()
        # Form field hovers to display live values
        temp_input = page.locator('input[cam-variable-name="currentTemp"], input[name="currentTemp"]').first
        if temp_input.is_visible():
            box = temp_input.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
                page.wait_for_timeout(600)

        # Select Stay Style (Lakeside Mökki)
        stay_select = page.locator('select[cam-variable-name="stayStyle"], select[name="stayStyle"]').first
        if stay_select.is_visible():
            box = stay_select.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
            stay_select.select_option("mökki")
            page.wait_for_timeout(500)

        # Select Dining Style (Campfire Cooking)
        dining_select = page.locator('select[cam-variable-name="diningStyle"], select[name="diningStyle"]').first
        if dining_select.is_visible():
            box = dining_select.bounding_box()
            if box:
                page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
            dining_select.select_option("campfire")
            page.wait_for_timeout(500)

        # Check confirmTrip if present
        confirm_cb = page.locator('input[type="checkbox"][cam-variable-name="confirmTrip"], input[type="checkbox"][name="confirmTrip"]').first
        if confirm_cb.is_visible():
            if not confirm_cb.is_checked():
                confirm_cb.click()
                page.wait_for_timeout(300)

        # Click Complete
        complete_btn = page.locator('button:has-text("Complete"), button[ng-click*="complete"]').first
        box = complete_btn.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        complete_btn.click()
        page.wait_for_timeout(2000)

    @keyword
    def open_mailpit_and_view_itinerary(self):
        """Navigates to Mailpit web UI and views the delivered adventure guide."""
        page = self._get_page()
        if "8025" not in page.url:
            page.goto("http://127.0.0.1:8025")
        page.wait_for_selector("body", timeout=5000)
        page.wait_for_timeout(800)
        msg_row = None
        for _ in range(25):
            candidate = page.get_by_text("Finnish Adventure Guide").first
            if candidate.is_visible():
                msg_row = candidate
                break
            page.wait_for_timeout(1000)

        if not msg_row:
            raise RuntimeError("Itinerary message not found in Mailpit UI")

        box = msg_row.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
        msg_row.click()
        page.wait_for_timeout(1200)
        # Scroll down slightly inside message preview to view itinerary details
        page.mouse.wheel(0, 300)
        page.wait_for_timeout(1500)
