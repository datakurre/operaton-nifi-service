#!/usr/bin/env python3
"""
Sets up the Finnish Travel & Outdoor Adventure Planner:
1. Deploys deployment/finnish-travel-planner.bpmn to Operaton.
2. Ensures Mailpit is running on 127.0.0.1:1025 / :8025.
3. Builds the 4-column NiFi pipeline in the 'Finnish Travel Planner' Process Group.
4. Optionally triggers and verifies a live trip (Koli preset or custom).
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

OPERATON_REST = os.environ.get("OPERATON_REST_URL", "http://127.0.0.1:8080/engine-rest")
NIFI_ADDR = os.environ.get("NIFI_ADDR", "http://127.0.0.1:8081")
MAILPIT_URL = os.environ.get("MAILPIT_URL", "http://127.0.0.1:8025")
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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


def ensure_mailpit():
    print("Checking Mailpit on http://127.0.0.1:8025 and 127.0.0.1:1025...")
    try:
        request(f"{MAILPIT_URL}/api/v1/messages")
        print("Mailpit is already running.")
        return
    except Exception:
        pass

    pid_file = "/tmp/agent-sandbox-mailpit.pid"
    log_file = "/tmp/agent-sandbox-mailpit.log"
    cmd = (
        f"nohup mailpit --listen 127.0.0.1:8025 --smtp 127.0.0.1:1025 "
        f"--disable-version-check --quiet >{log_file} 2>&1 & echo $! >{pid_file}"
    )
    subprocess.run(cmd, shell=True, check=True)
    for _ in range(15):
        try:
            request(f"{MAILPIT_URL}/api/v1/messages")
            print("Mailpit started successfully.")
            return
        except Exception:
            time.sleep(0.5)
    print("Warning: Mailpit check timed out.")


def deploy_bpmn():
    print("Deploying finnish-travel-planner.bpmn to Operaton...")
    auth = base64.b64encode(b"admin:admin").decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}
    bpmn_path = os.path.join(ROOT_DIR, "deployment", "finnish-travel-planner.bpmn")
    with open(bpmn_path, "rb") as f:
        bpmn_bytes = f.read()

    boundary = f"----OperatonFormBoundary{uuid.uuid4().hex}"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="deployment-name"\r\n\r\n'
        f"finnish-travel-planner\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="deploy-changed-only"\r\n\r\n'
        f"true\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="data"; filename="finnish-travel-planner.bpmn"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode("utf-8") + bpmn_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")

    deploy_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    res = request(f"{OPERATON_REST}/deployment/create", data=body, headers=deploy_headers, method="POST")
    print("Successfully deployed finnish-travel-planner.bpmn to Operaton.")
    return res


def configure_nifi():
    print("Configuring NiFi Flow for Finnish Travel Planner...")
    root = request(f"{NIFI_ADDR}/nifi-api/flow/process-groups/root")
    root_id = root["processGroupFlow"]["id"]

    # 1. Locate Operaton External Task Controller Service
    svcs = request(f"{NIFI_ADDR}/nifi-api/flow/process-groups/{root_id}/controller-services")
    operaton_svc = next(
        (s for s in svcs.get("controllerServices", []) if s["component"]["name"] == "StandardOperatonExternalTaskService"),
        None,
    )
    if not operaton_svc:
        raise RuntimeError("StandardOperatonExternalTaskService not found in NiFi root group.")
    operaton_svc_id = operaton_svc["id"]

    # 2. Check if 'Finnish Travel Planner' process group already exists
    groups = root["processGroupFlow"]["flow"]["processGroups"]
    travel_pg = next((g for g in groups if g["component"]["name"] == "Finnish Travel Planner"), None)

    if travel_pg:
        travel_pg_id = travel_pg["id"]
        print(f"Found existing 'Finnish Travel Planner' process group ({travel_pg_id}).")
        # Stop components inside
        try:
            request(
                f"{NIFI_ADDR}/nifi-api/flow/process-groups/{travel_pg_id}",
                data={"id": travel_pg_id, "state": "STOPPED"},
                method="PUT",
            )
            time.sleep(1)
        except Exception:
            pass

        # Purge and remove child connections and processors
        pg_flow = request(f"{NIFI_ADDR}/nifi-api/flow/process-groups/{travel_pg_id}")
        for conn in pg_flow["processGroupFlow"]["flow"]["connections"]:
            cid = conn["id"]
            try:
                request(f"{NIFI_ADDR}/nifi-api/flowfile-queues/{cid}/drop-requests", data=b"", method="POST")
            except Exception:
                pass
            try:
                c_fresh = request(f"{NIFI_ADDR}/nifi-api/connections/{cid}")
                ver = c_fresh["revision"]["version"]
                request(f"{NIFI_ADDR}/nifi-api/connections/{cid}?version={ver}", method="DELETE")
            except Exception:
                pass
        for proc in pg_flow["processGroupFlow"]["flow"]["processors"]:
            pid = proc["id"]
            try:
                p_fresh = request(f"{NIFI_ADDR}/nifi-api/processors/{pid}")
                ver = p_fresh["revision"]["version"]
                request(f"{NIFI_ADDR}/nifi-api/processors/{pid}?version={ver}", method="DELETE")
            except Exception:
                pass
    else:
        # Create Process Group
        pg_res = request(
            f"{NIFI_ADDR}/nifi-api/process-groups/{root_id}/process-groups",
            data={
                "revision": {"version": 0},
                "component": {
                    "name": "Finnish Travel Planner",
                    "position": {"x": 1200, "y": 40},
                },
            },
            method="POST",
        )
        travel_pg_id = pg_res["id"]
        print(f"Created 'Finnish Travel Planner' process group ({travel_pg_id}).")

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

    def create_p(name, ptype, bundle, x, y, props, auto_term=None):
        cfg = {"properties": props}
        if auto_term:
            cfg["autoTerminatedRelationships"] = auto_term
        res = request(
            f"{NIFI_ADDR}/nifi-api/process-groups/{travel_pg_id}/processors",
            data={
                "revision": {"version": 0},
                "component": {
                    "type": ptype,
                    "name": name,
                    "position": {"x": x, "y": y},
                    "bundle": bundle,
                    "config": cfg,
                },
            },
            method="POST",
        )
        return res["id"]

    def connect(src_id, dst_id, rels):
        request(
            f"{NIFI_ADDR}/nifi-api/process-groups/{travel_pg_id}/connections",
            data={
                "revision": {"version": 0},
                "component": {
                    "source": {"id": src_id, "type": "PROCESSOR", "groupId": travel_pg_id},
                    "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": travel_pg_id},
                    "selectedRelationships": rels,
                },
            },
            method="POST",
        )

    # =========================================================================
    # Column 1 (x=80): Destination Geocoding & Validation (fin-trip-lookup)
    # =========================================================================
    p_fetch_geo = create_p(
        "Fetch Destination Task",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        80, 40,
        {
            "External Task Service": operaton_svc_id,
            "Topic Name": "fin-trip-lookup",
        },
    )
    p_extract_trip = create_p(
        "Extract Trip Inputs",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        80, 240,
        {
            "Destination": "flowfile-attribute",
            "trip.preset": "$.presetDestination",
            "trip.custom": "$.customCity",
            "travelerName": "$.travelerName",
            "travelerEmail": "$.travelerEmail",
        },
        auto_term=["failure", "unmatched"],
    )
    p_resolve_city = create_p(
        "Resolve City Name",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        update_attr_bundle,
        80, 440,
        {
            "destinationName": "${trip.preset:equals('koli'):ifElse('Koli', ${trip.preset:equals('tampere'):ifElse('Tampere', ${trip.preset:equals('rovaniemi'):ifElse('Rovaniemi', ${trip.preset:equals('nuuksio'):ifElse('Nuuksio', ${trip.custom})})})})}",
            "search.city": "${trip.preset:equals('koli'):ifElse('Lieksa', ${trip.preset:equals('tampere'):ifElse('Tampere', ${trip.preset:equals('rovaniemi'):ifElse('Rovaniemi', ${trip.preset:equals('nuuksio'):ifElse('Espoo', ${trip.custom})})})})}",
            "travelerEmail": "${travelerEmail:isEmpty():ifElse('ada.lovelace@example.test', ${travelerEmail})}",
            "travelerName": "${travelerName:isEmpty():ifElse('Ada Lovelace', ${travelerName})}",
        },
    )
    p_invoke_geo = create_p(
        "Geocode Open-Meteo",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        standard_bundle,
        80, 640,
        {
            "HTTP Method": "GET",
            "HTTP URL": "https://geocoding-api.open-meteo.com/v1/search?name=${search.city:urlEncode()}&count=1",
            "Request User-Agent": "FinnishTravelPlanner/1.0 (contact: travel@example.test)",
        },
        auto_term=["Original", "Failure", "Retry", "No Retry"],
    )
    p_parse_geo = create_p(
        "Parse Geocode Coordinates",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        80, 840,
        {
            "Destination": "flowfile-attribute",
            "destinationLat": "$.results[0].latitude",
            "destinationLon": "$.results[0].longitude",
            "destinationCountry": "$.results[0].country",
            "destinationTimezone": "$.results[0].timezone",
        },
        auto_term=["failure"],
    )
    p_route_geo = create_p(
        "Validate Destination Location",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        standard_bundle,
        80, 1040,
        {
            "Routing Strategy": "Route to Property name",
            "valid": "${destinationLat:isEmpty():not():and(${destinationCountry:equals('Finland')})}",
        },
    )
    p_json_geo = create_p(
        "Prepare Lookup Variables JSON",
        "org.apache.nifi.processors.standard.AttributesToJSON",
        standard_bundle,
        80, 1240,
        {
            "Attributes List": "destinationName,destinationLat,destinationLon,destinationCountry,destinationTimezone,travelerName,travelerEmail",
            "Destination": "flowfile-content",
            "Include Core Attributes": "false",
            "Null Value": "false",
        },
        auto_term=["failure"],
    )
    p_complete_geo = create_p(
        "Complete Destination Lookup",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        80, 1440,
        {
            "External Task Service": operaton_svc_id,
            "Variables Format": "SIMPLE",
        },
        auto_term=["success", "failure"],
    )
    p_err_geo = create_p(
        "Handle Unknown Destination",
        "org.operaton.bpm.extension.nifi.processor.HandleExternalTaskBpmnError",
        operaton_bundle,
        360, 1040,
        {
            "External Task Service": operaton_svc_id,
            "Error Code": "UNKNOWN_DESTINATION",
            "Error Message": "Could not resolve Finnish destination location for: ${search.city}",
        },
        auto_term=["success", "failure"],
    )

    connect(p_fetch_geo, p_extract_trip, ["success"])
    connect(p_extract_trip, p_resolve_city, ["matched"])
    connect(p_resolve_city, p_invoke_geo, ["success"])
    connect(p_invoke_geo, p_parse_geo, ["Response"])
    connect(p_parse_geo, p_route_geo, ["matched", "unmatched"])
    connect(p_route_geo, p_json_geo, ["valid"])
    connect(p_json_geo, p_complete_geo, ["success"])
    connect(p_route_geo, p_err_geo, ["unmatched"])

    # Fallback suggestion task
    p_fetch_fallback = create_p(
        "Fetch Fallback Task",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        360, 1040,
        {
            "External Task Service": operaton_svc_id,
            "Topic Name": "fin-destination-fallback",
        },
    )
    p_email_fallback = create_p(
        "Email Fallback Notice",
        "org.apache.nifi.processors.standard.PutEmail",
        standard_bundle,
        360, 1240,
        {
            "SMTP Hostname": "127.0.0.1",
            "SMTP Port": "1025",
            "SMTP Auth": "false",
            "From": "adventure@finland-travel.local",
            "To": "ada.lovelace@example.test",
            "Subject": "Destination Not Found - Try Iconic Finnish Presets",
            "Message": "Hello!\n\nWe could not resolve your requested destination. Please try one of our popular Finnish presets:\n- Koli National Park (North Karelia)\n- Tampere (Sauna Capital)\n- Rovaniemi & Lapland\n- Nuuksio National Park\n\nHappy travels!",
        },
        auto_term=["failure"],
    )
    p_complete_fallback = create_p(
        "Complete Fallback Task",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        360, 1440,
        {
            "External Task Service": operaton_svc_id,
            "Variables Format": "NONE",
        },
        auto_term=["success", "failure"],
    )
    connect(p_fetch_fallback, p_email_fallback, ["success"])
    connect(p_email_fallback, p_complete_fallback, ["success"])

    # =========================================================================
    # Column 2 (x=640): FMI Weather & Cultural Bio (fin-weather-check)
    # =========================================================================
    p_fetch_weather = create_p(
        "Fetch Weather Task",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        640, 40,
        {
            "External Task Service": operaton_svc_id,
            "Topic Name": "fin-weather-check",
        },
    )
    p_extract_coords = create_p(
        "Extract Destination Coordinates",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        640, 240,
        {
            "Destination": "flowfile-attribute",
            "dest.name": "$.destinationName",
            "dest.lat": "$.destinationLat",
            "dest.lon": "$.destinationLon",
        },
        auto_term=["failure", "unmatched"],
    )
    p_invoke_weather = create_p(
        "Fetch Open-Meteo FMI Weather",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        standard_bundle,
        640, 440,
        {
            "HTTP Method": "GET",
            "HTTP URL": "https://api.open-meteo.com/v1/forecast?latitude=${dest.lat}&longitude=${dest.lon}&current=temperature_2m,weather_code,wind_speed_10m&daily=sunrise,sunset&timezone=auto",
            "Request User-Agent": "FinnishTravelPlanner/1.0 (contact: travel@example.test)",
        },
        auto_term=["Original", "Failure", "Retry", "No Retry"],
    )
    p_parse_weather = create_p(
        "Parse Weather Stats",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        640, 640,
        {
            "Destination": "flowfile-attribute",
            "weather.temp": "$.current.temperature_2m",
            "weather.code": "$.current.weather_code",
            "weather.wind": "$.current.wind_speed_10m",
            "weather.sunrise": "$.daily.sunrise[0]",
            "weather.sunset": "$.daily.sunset[0]",
        },
        auto_term=["failure", "unmatched"],
    )
    p_invoke_wiki = create_p(
        "Fetch Wikipedia Profile",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        standard_bundle,
        640, 840,
        {
            "HTTP Method": "GET",
            "HTTP URL": "https://en.wikipedia.org/api/rest_v1/page/summary/${dest.name:equals('Nuuksio'):ifElse('Nuuksio_National_Park', ${dest.name:equals('Koli'):ifElse('Koli_National_Park', ${dest.name:urlEncode()})})}",
            "Request User-Agent": "FinnishTravelPlanner/1.0 (contact: travel@example.test)",
        },
        auto_term=["Original", "Retry"],
    )
    p_parse_wiki = create_p(
        "Parse Cultural Extract",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        640, 1040,
        {
            "Destination": "flowfile-attribute",
            "wiki.extract": "$.extract",
            "wiki.thumbnail": "$.thumbnail.source",
            "wiki.description": "$.description",
        },
        auto_term=["failure"],
    )
    p_format_dossier = create_p(
        "Format Destination Dossier",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        update_attr_bundle,
        640, 1240,
        {
            "currentTemp": "${weather.temp} °C",
            "weatherCondition": "${weather.code:isEmpty():ifElse('☀️ Crisp Nordic Weather', ${weather.code:le(0):ifElse('☀️ Clear Skies & Crisp Air', ${weather.code:le(3):ifElse('⛅ Partly Cloudy & Fresh Air', ${weather.code:ge(71):ifElse('❄️ Snowing / Winter Wonder', '🌧️ Fresh Nordic Showers')})})})}",
            "daylightHours": "Sunrise ${weather.sunrise:substringAfter('T')} – Sunset ${weather.sunset:substringAfter('T')}",
            "destinationDescription": "${wiki.description:isEmpty():ifElse(${wiki.extract:isEmpty():ifElse('Breathtaking Finnish wilderness, serene lakes and untouched nature.', ${wiki.extract:substring(0, 120)})}, ${wiki.description})}",
            "destinationPhoto": "${wiki.thumbnail:isEmpty():ifElse('https://upload.wikimedia.org/wikipedia/commons/thumb/7/7f/Koli_hill_view.jpg/330px-Koli_hill_view.jpg', ${wiki.thumbnail})}",
        },
    )
    p_json_dossier = create_p(
        "Prepare Dossier Variables JSON",
        "org.apache.nifi.processors.standard.AttributesToJSON",
        standard_bundle,
        640, 1440,
        {
            "Attributes List": "currentTemp,weatherCondition,daylightHours,destinationDescription,destinationPhoto",
            "Destination": "flowfile-content",
            "Include Core Attributes": "false",
            "Null Value": "false",
        },
        auto_term=["failure"],
    )
    p_complete_weather = create_p(
        "Complete Weather & Dossier Task",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        640, 1640,
        {
            "External Task Service": operaton_svc_id,
            "Variables Format": "SIMPLE",
        },
        auto_term=["success", "failure"],
    )

    connect(p_fetch_weather, p_extract_coords, ["success"])
    connect(p_extract_coords, p_invoke_weather, ["matched"])
    connect(p_invoke_weather, p_parse_weather, ["Response"])
    connect(p_parse_weather, p_invoke_wiki, ["matched"])
    connect(p_invoke_wiki, p_parse_wiki, ["Response"])
    connect(p_invoke_wiki, p_format_dossier, ["Failure", "No Retry"])
    connect(p_parse_wiki, p_format_dossier, ["matched", "unmatched"])
    connect(p_format_dossier, p_json_dossier, ["success"])
    connect(p_json_dossier, p_complete_weather, ["success"])

    # =========================================================================
    # Column 3 (x=1200): Itinerary Compilation & Dining (fin-itinerary-compile)
    # =========================================================================
    p_fetch_compile = create_p(
        "Fetch Itinerary Compile Task",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        1200, 40,
        {
            "External Task Service": operaton_svc_id,
            "Topic Name": "fin-itinerary-compile",
        },
    )
    p_extract_user = create_p(
        "Extract User Choices",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        1200, 240,
        {
            "Destination": "flowfile-attribute",
            "user.dest": "$.destinationName",
            "user.season": "$.seasonVibe",
            "user.stay": "$.stayStyle",
            "user.dining": "$.diningStyle",
            "user.dietary": "$.dietaryNotes",
            "user.notes": "$.specialNotes",
            "user.temp": "$.currentTemp",
            "user.weather": "$.weatherCondition",
            "user.daylight": "$.daylightHours",
            "user.traveler": "$.travelerName",
            "user.email": "$.travelerEmail",
        },
        auto_term=["failure", "unmatched"],
    )
    p_map_dining = create_p(
        "Map Regional Food & Gear",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        update_attr_bundle,
        1200, 440,
        {
            "food.lunch": "${user.dest:equals('Koli'):ifElse('Warm Karjalanpiirakat (Karelian rice pies with egg butter) + campfire grilled sausages on Lake Pielinen shore', ${user.dest:equals('Tampere'):ifElse('Authentic hot Mustamakkara with tart lingonberry jam & fresh milk at Tammelantori Market', ${user.dest:equals('Rovaniemi'):ifElse('Hearty Lohikeitto (creamy salmon soup loaded with fresh dill and butter) with dense sourdough rye bread', 'Loimulohi (flame-cooked plank salmon) with pickled cucumbers by the lake')})})}",
            "food.fika": "${user.dest:equals('Koli'):ifElse('Nokipannukahvi (campfire kettle coffee) with warm leipäjuusto (squeaky cheese) & cloudberries', ${user.dest:equals('Tampere'):ifElse('World-famous hot cardamom donuts (munkki) at the historic Pyynikki Observation Tower Cafe', ${user.dest:equals('Rovaniemi'):ifElse('Spiced hot berry glögi with fresh gingerbread cookies and arctic cloudberries', 'Giant fresh cinnamon-cardamom bun (korvapuusti) & dark roast coffee at Haltia Nature Centre')})})}",
            "food.dinner": "${user.dest:equals('Koli'):ifElse('Crispy pan-fried savumuikut (smoked vendace) with crushed dill potatoes and chanterelle mushroom cream sauce', ${user.dest:equals('Tampere'):ifElse('Wood-fired local dinner and craft cider at Saunaravintola Kuuma overlooking the harbor', ${user.dest:equals('Rovaniemi'):ifElse('Traditional Poronkäristys (sautéed reindeer) over velvety mashed potatoes with crisp sour pickles & lingonberries at Ravintola Nili', 'Wild mushroom & foraged seasonal herb dinner at a cozy lakeside lodge')})})}",
            "gear.checklist": "Merino wool base layer, windproof shell jacket, waterproof hiking boots, thermos bottle, puukko knife & matches for campfire, sauna towel & slippers",
        },
    )
    p_render_itinerary = create_p(
        "Render Adventure Markdown",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        update_attr_bundle,
        1200, 640,
        {
            "itineraryMarkdown": (
                "# 🌲 Your Finnish Adventure Briefing: ${user.dest}\n\n"
                "**Traveler**: ${user.traveler}\n"
                "**Current Weather**: ${user.temp} (${user.weather})\n"
                "**Daylight Window**: ${user.daylight}\n\n"
                "---\n\n"
                "## 🗺️ Daily Schedule & Route\n"
                "- **08:30**: Breakfast & morning gear prep.\n"
                "- **09:30 – 12:30**: Scenic trail hike & panoramic nature viewpoints.\n"
                "- **13:00 – 14:00**: 🪵 **Lunch Spotlight**: ${food.lunch}\n"
                "- **15:00**: ☕ **Afternoon Fika / Kahvit**: ${food.fika}\n"
                "- **16:30 – 18:30**: ♨️ **Wood-Fired Sauna & Cold Lake Plunge** (Löyly rounds & lake dip).\n"
                "- **19:00**: 🍽️ **Dinner**: ${food.dinner}\n\n"
                "---\n\n"
                "## 🎒 Finnish Outdoor & Sauna Gear Checklist\n"
                "- [x] ${gear.checklist}\n\n"
                "*Special Notes*: ${user.notes}\n"
            ),
        },
    )
    p_json_itinerary = create_p(
        "Prepare Itinerary Variables JSON",
        "org.apache.nifi.processors.standard.AttributesToJSON",
        standard_bundle,
        1200, 840,
        {
            "Attributes List": "itineraryMarkdown",
            "Destination": "flowfile-content",
            "Include Core Attributes": "false",
            "Null Value": "false",
        },
        auto_term=["failure"],
    )
    p_complete_compile = create_p(
        "Complete Itinerary Compile",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        1200, 1040,
        {
            "External Task Service": operaton_svc_id,
            "Variables Format": "SIMPLE",
        },
        auto_term=["success", "failure"],
    )

    connect(p_fetch_compile, p_extract_user, ["success"])
    connect(p_extract_user, p_map_dining, ["matched"])
    connect(p_map_dining, p_render_itinerary, ["success"])
    connect(p_render_itinerary, p_json_itinerary, ["success"])
    connect(p_json_itinerary, p_complete_compile, ["success"])

    # =========================================================================
    # Column 4 (x=1760): Dispatch to Mailpit & File Archive (fin-itinerary-dispatch)
    # =========================================================================
    p_fetch_dispatch = create_p(
        "Fetch Dispatch Task",
        "org.operaton.bpm.extension.nifi.processor.FetchAndLockExternalTasks",
        operaton_bundle,
        1760, 40,
        {
            "External Task Service": operaton_svc_id,
            "Topic Name": "fin-itinerary-dispatch",
        },
    )
    p_extract_dispatch = create_p(
        "Extract Dispatch Content",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        standard_bundle,
        1760, 240,
        {
            "Destination": "flowfile-attribute",
            "itineraryMarkdown": "$.itineraryMarkdown",
            "destinationName": "$.destinationName",
            "travelerEmail": "$.travelerEmail",
        },
        auto_term=["failure", "unmatched"],
    )
    p_set_email_body = create_p(
        "Set Markdown as FlowFile Content",
        "org.apache.nifi.processors.standard.ReplaceText",
        standard_bundle,
        1760, 440,
        {
            "Replacement Strategy": "Always Replace",
            "Replacement Value": "${itineraryMarkdown}",
        },
        auto_term=["failure"],
    )
    p_set_file_attr = create_p(
        "Set File and Email Attributes",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        update_attr_bundle,
        1760, 640,
        {
            "filename": "${destinationName:toLower()}-adventure-guide.md",
            "emailTo": "${travelerEmail:isEmpty():ifElse('ada.lovelace@example.test', ${travelerEmail})}",
        },
    )
    itinerary_dir = os.path.join(ROOT_DIR, "deployment", "itineraries")
    os.makedirs(itinerary_dir, exist_ok=True)
    p_email_itinerary = create_p(
        "Deliver Itinerary to Mailpit",
        "org.apache.nifi.processors.standard.PutEmail",
        standard_bundle,
        1760, 840,
        {
            "SMTP Hostname": "127.0.0.1",
            "SMTP Port": "1025",
            "SMTP Auth": "false",
            "email-ff-content-as-message": "true",
            "From": "adventure@finland-travel.local",
            "To": "${emailTo}",
            "Subject": "Your Finnish Adventure Guide: ${destinationName}",
        },
        auto_term=["failure"],
    )
    p_archive_file = create_p(
        "Archive Itinerary Markdown",
        "org.apache.nifi.processors.standard.PutFile",
        standard_bundle,
        1760, 1040,
        {
            "Directory": itinerary_dir,
            "Conflict Resolution Strategy": "replace",
        },
        auto_term=["failure"],
    )
    p_complete_dispatch = create_p(
        "Complete Dispatch Task",
        "org.operaton.bpm.extension.nifi.processor.CompleteExternalTask",
        operaton_bundle,
        1760, 1240,
        {
            "External Task Service": operaton_svc_id,
            "Variables Format": "NONE",
        },
        auto_term=["success", "failure"],
    )

    connect(p_fetch_dispatch, p_extract_dispatch, ["success"])
    connect(p_extract_dispatch, p_set_email_body, ["matched"])
    connect(p_set_email_body, p_set_file_attr, ["success"])
    connect(p_set_file_attr, p_email_itinerary, ["success"])
    connect(p_email_itinerary, p_archive_file, ["success"])
    connect(p_archive_file, p_complete_dispatch, ["success"])

    # Start all processors in the process group
    request(
        f"{NIFI_ADDR}/nifi-api/flow/process-groups/{travel_pg_id}",
        data={"id": travel_pg_id, "state": "RUNNING"},
        method="PUT",
    )
    print("All processors in 'Finnish Travel Planner' process group started successfully.")


def run_test_instance(preset="koli", custom_city=""):
    print(f"\n--- Running Live Test Instance for preset: {preset} (custom_city: {custom_city}) ---")
    auth = base64.b64encode(b"admin:admin").decode("ascii")
    headers = {"Authorization": f"Basic {auth}"}

    # 1. Start process instance
    biz_key = f"TRIP-{(custom_city or preset).upper()}-{int(time.time())}"
    start_res = request(
        f"{OPERATON_REST}/process-definition/key/finnish-travel-planner/start",
        data={
            "businessKey": biz_key,
            "variables": {
                "presetDestination": {"value": "custom" if custom_city else preset, "type": "String"},
                "customCity": {"value": custom_city, "type": "String"},
                "travelerName": {"value": "Ada Lovelace", "type": "String"},
                "travelerEmail": {"value": "ada.lovelace@example.test", "type": "String"},
            },
        },
        headers=headers,
        method="POST",
    )
    inst_id = start_res["id"]
    print(f"Started process instance {inst_id} (businessKey: {biz_key}).")

    if custom_city:
        print(f"Testing invalid/custom destination error fallback for: {custom_city}...")
        latest = None
        for _ in range(15):
            time.sleep(1)
            msgs = request(f"{MAILPIT_URL}/api/v1/messages")
            matching = [
                m for m in msgs.get("messages", [])
                if "Destination Not Found" in m.get("Subject", "")
            ]
            if matching:
                latest = matching[0]
                break
        if latest:
            print(f"\n[SUCCESS] Found delivered fallback suggestions email in Mailpit!")
            print(f"  • Subject: {latest['Subject']}")
            print(f"  • To: {latest['To'][0]['Address']}")
            print(f"  • Snippet: {latest.get('Snippet', '')}")
            return
        else:
            raise RuntimeError("Could not locate fallback suggestions email in Mailpit.")

    # 2. Wait for token to reach User Task 'UserTask_CurateTrip'
    print("Waiting for NiFi to resolve geocoding, FMI weather & Wikipedia dossier...")
    user_task = None
    for i in range(60):
        tasks = request(
            f"{OPERATON_REST}/task?processInstanceId={inst_id}&taskDefinitionKey=UserTask_CurateTrip",
            headers=headers,
        )
        if tasks:
            user_task = tasks[0]
            break
        if (i + 1) % 5 == 0:
            print(f"  ... still waiting ({i + 1}s)")
        time.sleep(1)

    if not user_task:
        raise RuntimeError("Timed out waiting for User Task to become active.")

    print(f"User Task 'UserTask_CurateTrip' is active (Task ID: {user_task['id']})!")

    # Check pre-populated variables
    vars_data = request(f"{OPERATON_REST}/task/{user_task['id']}/form-variables", headers=headers)
    print("Pre-populated Form Variables:")
    for k in ["destinationName", "currentTemp", "weatherCondition", "daylightHours", "destinationPhoto"]:
        val = vars_data.get(k, {}).get("value")
        print(f"  • {k}: {val}")

    # 3. Complete User Task with choices
    print("\nCompleting User Task with traveler choices (Season: Ruska, Mökki, Campfire dining)...")
    request(
        f"{OPERATON_REST}/task/{user_task['id']}/complete",
        data={
            "variables": {
                "seasonVibe": {"value": "ruska", "type": "String"},
                "stayStyle": {"value": "mökki", "type": "String"},
                "diningStyle": {"value": "campfire", "type": "String"},
                "dietaryNotes": {"value": "Fish & local berry desserts", "type": "String"},
                "specialNotes": {"value": "Campfire coffee on Lake Pielinen shore.", "type": "String"},
                "confirmTrip": {"value": True, "type": "Boolean"},
            }
        },
        headers=headers,
        method="POST",
    )
    print("User Task completed!")

    # 4. Wait for NiFi to compile itinerary and dispatch to Mailpit
    print("Waiting for NiFi to compile itinerary and deliver email to Mailpit...")
    latest = None
    for _ in range(15):
        time.sleep(1)
        msgs = request(f"{MAILPIT_URL}/api/v1/messages")
        matching = [
            m for m in msgs.get("messages", [])
            if "Finnish Adventure Guide" in m.get("Subject", "")
        ]
        if matching:
            latest = matching[0]
            break

    if latest:
        print(f"\n[SUCCESS] Found delivered email in Mailpit!")
        print(f"  • Subject: {latest['Subject']}")
        print(f"  • To: {latest['To'][0]['Address']}")
        print(f"  • Snippet: {latest.get('Snippet', '')}")
    else:
        raise RuntimeError("Could not locate delivered itinerary email in Mailpit.")

    # 6. Check archived markdown file
    dest_dir = os.path.join(ROOT_DIR, "deployment", "itineraries")
    if os.path.exists(dest_dir):
        files = os.listdir(dest_dir)
        print(f"\nArchived itineraries on disk: {files}")

    print("\nLive test instance finished successfully!")


def main():
    parser = argparse.ArgumentParser(description="Setup Finnish Travel Planner")
    parser.add_argument("--test", action="store_true", help="Run end-to-end test instance after setup")
    parser.add_argument("--preset", default="koli", choices=["koli", "tampere", "rovaniemi", "nuuksio"])
    parser.add_argument("--custom", default="", help="Custom city name to test (e.g. invalid city for BPMN error)")
    args = parser.parse_args()

    ensure_mailpit()
    deploy_bpmn()
    configure_nifi()
    if args.test:
        run_test_instance(preset=args.preset, custom_city=args.custom)


if __name__ == "__main__":
    main()
