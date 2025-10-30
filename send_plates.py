import os
import json
import traceback
from flask import Flask, request, jsonify, Response, render_template_string
import requests
from shotgun_api3 import Shotgun
from base64 import b64encode

app = Flask(__name__)

# ---------------------------
# Configuration / Mapping
# ---------------------------

SG_URL = os.environ.get("SG_URL")
SG_SCRIPT_NAME = os.environ.get("SG_SCRIPT_NAME")
SG_SCRIPT_KEY = os.environ.get("SG_SCRIPT_KEY")

FMP_BASE_URL = os.environ.get("FMP_BASE_URL")  # e.g. https://filemaker.example.com
FMP_DATABASE = os.environ.get("FMP_DATABASE")  # db name
FMP_LAYOUT = os.environ.get("FMP_LAYOUT")      # layout name for creating records
FMP_USER = os.environ.get("FMP_USER")
FMP_PASSWORD = os.environ.get("FMP_PASSWORD")

DEBUG = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")

# Field mapping derived from your CSV.
# dict: SG field code -> FMP field name
FIELD_MAP = {
    "sg_latest_version": "Plate Name",
    "sg_slate": "Slate",
    "sg_camera_file_name": "Source File Name",
    "sg_source_in": "Timecode In",
    "sg_source_out": "Timecode Out",
    "sg_turnover": "Turnover Package",
    "sg_head_in": "Head In",
    "sg_cut_in": "Cut In",
    "sg_cut_out": "Cut Out",
    "sg_tail_out": "Tail Out",
    "sg_lut": "LUT",
    "description": "Notes",
    # shot.id is special / nested - we'll include shot in query and map shot.id -> ForeignKey
    "shot": "ForeignKey",
}

# If the mapping uses a special key referring to nested values, handle here:
SPECIAL_SG_KEYS = {
    # if we query "shot" then we will extract shot['id'] into the FMP ForeignKey field
    "shot": ("ForeignKey", lambda sg_val: sg_val["id"] if isinstance(sg_val, dict) else sg_val),
}

# ---------------------------
# Helpers
# ---------------------------

def log(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

def get_shotgun():
    if not (SG_URL and SG_SCRIPT_NAME and SG_SCRIPT_KEY):
        raise RuntimeError("ShotGrid credentials not set (SG_URL / SG_SCRIPT_NAME / SG_SCRIPT_KEY).")
    return Shotgun(SG_URL, SG_SCRIPT_NAME, SG_SCRIPT_KEY)

def build_fields_to_query():
    """
    Build a list of SG field codes to request from ShotGrid based on FIELD_MAP.
    For nested/special keys, include the high-level field code (e.g. 'shot').
    """
    fields = []
    for sg_key in FIELD_MAP.keys():
        # if sg_key is 'shot' we query 'shot' (ShotGrid returns a dict)
        fields.append(sg_key)
    # Always include id for debugging / reference
    if "id" not in fields:
        fields.append("id")
    return list(dict.fromkeys(fields))  # remove duplicates while preserving order

def fm_get_token():
    """Authenticate and return FMP session token"""
    sess_url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/sessions"
    auth_string = f"{FMP_USER}:{FMP_PASSWORD}"
    auth_base64 = b64encode(auth_string.encode("utf-8")).decode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_base64}",
    }

    r = requests.post(sess_url, headers=headers)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create FileMaker session: {r.status_code} {r.text}")

    data = r.json()
    token = data.get("response", {}).get("token")
    if not token:
        raise RuntimeError(f"No token found in FileMaker session response: {data}")
    return token

def fm_create_records(token, records_payload):
    """
    POST records to FileMaker layout.
    Endpoint: POST /fmi/data/vLatest/databases/{db}/layouts/{layout}/records
    """
    url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/layouts/{FMP_LAYOUT}/records"
        headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {"records": records_payload}
    r = requests.post(url, headers=headers, json=payload)
    return r

def fm_close_session(token):
    url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/sessions/{token}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        requests.delete(url, headers=headers)
    except Exception:
        pass

def sg_value_to_fmp_value(sg_field_code, sg_value):
    """
    Convert the SG field value into an FMP-friendly representation.
    Handles simple types, and specially handles entity refs (dicts).
    """
    # handle special mapping defined above
    if sg_field_code in SPECIAL_SG_KEYS:
        fmp_field, transform = SPECIAL_SG_KEYS[sg_field_code]
        try:
            return transform(sg_value)
        except Exception:
            return None

    # For typical ShotGrid types:
    # - dict (entity ref) -> use id or name? We generally want the name unless mapping expects id.
    if isinstance(sg_value, dict):
        # prefer id if the mapping expects an id (we only special-case 'shot' above),
        # otherwise use 'name' if available
        return sg_value.get("name") or sg_value.get("id")

    # lists: join simple text fields
    if isinstance(sg_value, list):
        # flatten list of simple items
        out = []
        for it in sg_value:
            if isinstance(it, dict):
                out.append(it.get("name") or str(it.get("id")))
            else:
                out.append(str(it))
        return ", ".join(out)

    # everything else -> string or numeric as-is
    return sg_value

# ---------------------------
# Flask endpoint
# ---------------------------

@app.route("/send_plates", methods=["POST", "GET"])
def index():
    """
    POST payload expectations:
    - JSON: { "entity_type": "Element", "entity_ids": [123,456,...] }
    - Form: ids=123,456,789
    GET (for easy manual testing) accepts ?ids=123,456&entity_type=Element
    """
    try:
        # parse ids
        entity_type = request.values.get("entity_type", "Element")
        ids = None

        # JSON body?
        if request.is_json:
            body = request.get_json(silent=True) or {}
            ids = body.get("entity_ids") or body.get("ids")
            # If entity_ids come as comma-separated string
            if isinstance(ids, str):
                ids = [int(x) for x in ids.split(",") if x.strip()]
        else:
            # form / querystring
            ids_raw = request.values.get("ids")
            if ids_raw:
                ids = [int(x) for x in ids_raw.split(",") if x.strip()]

        if not ids:
            return jsonify({"ok": False, "error": "No entity IDs provided. Use JSON {entity_ids: [...] } or form/query ids=1,2,3"}), 400

        # Connect to ShotGrid and query elements
        sg = get_shotgun()
        fields = build_fields_to_query()
        log("Querying ShotGrid fields:", fields)
        # shotgun_api3 api: sg.find(entity_type, filters, fields)
        filters = [["id", "in", ids]]
        sg_results = sg.find(entity_type, filters, fields)
        log("Found", len(sg_results), "results")

        # Build records for FileMaker
        records_to_create = []
        created_meta = []
        skipped_count = 0
        for ent in sg_results:
            fieldData = {}
            for sg_key, fmp_field in FIELD_MAP.items():
                # if the field is 'shot' special mapping, we expect ent['shot'] to be present
                if sg_key not in ent:
                    # missing field in this element; skip
                    continue
                sg_val = ent.get(sg_key)
                # Transform nested or entity values
                if sg_key in SPECIAL_SG_KEYS:
                    try:
                        transformed = SPECIAL_SG_KEYS[sg_key][1](sg_val)
                    except Exception:
                        transformed = None
                    if transformed is None:
                        continue
                    fieldData[fmp_field] = transformed
                    continue

                # normal fields
                value = sg_value_to_fmp_value(sg_key, sg_val)
                # skip None and empty
                if value is None:
                    continue
                # final assignment
                fieldData[fmp_field] = value

            # if there's nothing to push for this record, skip
            if not fieldData:
                skipped_count += 1
                created_meta.append({"sg_id": ent.get("id"), "status": "skipped", "reason": "no mapped fields present"})
                continue

            # Optionally attach a reference to SG element id in the record so you can find it later
            # We'll include "SG_ID" if exists in your FMP layout (uncomment to include):
            # fieldData.setdefault("SG_ID", ent.get("id"))

            records_to_create.append({"fieldData": fieldData})
            created_meta.append({"sg_id": ent.get("id"), "status": "queued", "fields": list(fieldData.keys())})

        if not records_to_create:
            msg = {"ok": True, "created": 0, "skipped": skipped_count, "details": created_meta}
            if request.args.get("html") == "1":
                html = render_template_string(
                    "<h2>Element → FileMaker</h2><p>No records to create. Skipped {{skipped}}.</p>",
                    skipped=skipped_count
                )
                return Response(html, mimetype="text/html")
            return jsonify(msg)

        # --- SAFEGUARD: remove any empty or invalid records ---
        records_to_create = [r for r in records_to_create if "fieldData" in r and r["fieldData"]]

        if not records_to_create:
            msg = {
                "ok": True,
                "created": 0,
                "skipped": skipped_count,
                "details": created_meta,
                "error": "No records had fieldData to send to FileMaker"
            }
            if request.args.get("html") == "1":
                html = render_template_string(
                    "<h2>Element → FileMaker</h2><p>No records to create. Skipped {{skipped}}.</p>",
                    skipped=skipped_count
                )
                return Response(html, mimetype="text/html")
            return jsonify(msg)

        # Authenticate to FileMaker and create records
        token = None
        try:
            token = fm_get_token()
            log("Obtained FMP token:", token)
            # FileMaker has a limit for bulk creates - but most servers allow many. We'll post them all at once.
            r = fm_create_records(token, records_to_create)
            if r.status_code not in (200, 201):
                # Try to decode response body for diagnostics
                try:
                    body = r.json()
                except Exception:
                    body = r.text
                raise RuntimeError(f"FileMaker create failed {r.status_code}: {body}")

            resp_json = r.json()

            # Count successes vs failures using FM response structure:
            fm_created = 0
            fm_errors = []
            # response contains "response": {"data": [ ... ] }
            for idx, rec_resp in enumerate(resp_json.get("response", {}).get("data", [])):
                # each rec_resp might have "recordId" etc.
                fm_created += 1

            # Build reply
            result = {
                "ok": True,
                "requested": len(records_to_create),
                "created": fm_created,
                "skipped": skipped_count,
                "shotgrid_requested": len(sg_results),
                "details": created_meta,
                "fmp_response": resp_json.get("response", {})
            }

            # optionally return html
            if request.args.get("html") == "1":
                html_tpl = """
                <h2>Plates → FileMaker</h2>
                <p>Requested SG elements: {{sg_count}}</p>
                <p>Records sent to FM: {{sent}}</p>
                <p>Created in FM: {{created}}</p>
                <p>Skipped (no mapped fields): {{skipped}}</p>
                <hr>
                <h3>Details</h3>
                <ul>
                {% for d in details %}
                  <li>SG id {{d.sg_id}} — {{d.status}} {% if d.reason %} ({{d.reason}}) {% endif %}</li>
                {% endfor %}
                </ul>
                """
                html = render_template_string(html_tpl,
                                              sg_count=len(sg_results),
                                              sent=len(records_to_create),
                                              created=fm_created,
                                              skipped=skipped_count,
                                              details=created_meta)
                return Response(html, mimetype="text/html")

            return jsonify(result)

        finally:
            # always try to close FileMaker session token if created
            if token:
                try:
                    fm_close_session(token)
                    log("Closed FMP session.")
                except Exception:
                    pass

    except Exception as exc:
        log("Exception:", traceback.format_exc())
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=DEBUG)
