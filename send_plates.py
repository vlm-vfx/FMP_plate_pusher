import os
import json
import traceback
from flask import Flask, request, jsonify, Response
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

FMP_BASE_URL = os.environ.get("FMP_BASE_URL")
FMP_DATABASE = os.environ.get("FMP_DATABASE")
FMP_LAYOUT = os.environ.get("FMP_LAYOUT")
FMP_USER = os.environ.get("FMP_USER")
FMP_PASSWORD = os.environ.get("FMP_PASSWORD")

# Field mapping derived from your CSV.
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
    "shot": "ForeignKey",
}

# Special key transforms
SPECIAL_SG_KEYS = {
    "shot": ("ForeignKey", lambda sg_val: sg_val["id"] if isinstance(sg_val, dict) else sg_val),
}

# ---------------------------
# Helpers
# ---------------------------

def get_shotgun():
    if not (SG_URL and SG_SCRIPT_NAME and SG_SCRIPT_KEY):
        raise RuntimeError("ShotGrid credentials not set (SG_URL / SG_SCRIPT_NAME / SG_SCRIPT_KEY).")
    return Shotgun(SG_URL, SG_SCRIPT_NAME, SG_SCRIPT_KEY)

def build_fields_to_query():
    """
    Build a list of SG field codes to request from ShotGrid based on FIELD_MAP.
    Handles nested entity links properly (sg_latest_version and shot).
    """
    fields = []

    for sg_key in FIELD_MAP.keys():
        # For nested link fields, we’ll add the main entity field only.
        # We'll handle their subfields via 'additional_filter_presets' in the sg.find() call.
        if sg_key in ("sg_latest_version", "shot"):
            fields.append(sg_key)
        else:
            fields.append(sg_key)

    if "id" not in fields:
        fields.append("id")

    return list(dict.fromkeys(fields))  # remove duplicates while preserving order


def get_elements(sg, selected_ids=None):
    """
    Query ShotGrid for Elements, optionally limited to a selected list of IDs.
    """
    filters = []
    if selected_ids:
        filters = [["id", "in", selected_ids]]

    fields = build_fields_to_query()
    print(f"Querying ShotGrid fields: {fields}")

    # The 'additional_filter_presets' trick won't help here,
    # so we request nested subfields explicitly via 'fields' argument as dicts.
    # This is the correct way to fetch linked entity subfields like code/id.
    elements = sg.find(
        "Element",
        filters,
        fields,
        additional_fields=[
            {"field_name": "sg_latest_version", "sub_fields": ["code", "id"]},
            {"field_name": "shot", "sub_fields": ["code", "id"]},
        ],
    )

    print(f"Found {len(elements)} results")
    print("SG element fields for debug:")
    for el in elements:
        print(json.dumps(el, indent=2))

    return elements

def fm_get_token():
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
    token = r.json().get("response", {}).get("token")
    if not token:
        raise RuntimeError(f"No token found in FileMaker session response: {r.json()}")
    return token

def fm_create_records(token, records_payload):
    url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/layouts/{FMP_LAYOUT}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return requests.post(url, headers=headers, json={"records": records_payload})

def fm_close_session(token):
    url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/sessions/{token}"
    try:
        requests.delete(url, headers={"Authorization": f"Bearer {token}"})
    except Exception:
        pass

def sg_value_to_fmp_value(sg_field_code, sg_value):
    if sg_field_code in SPECIAL_SG_KEYS:
        fmp_field, transform = SPECIAL_SG_KEYS[sg_field_code]
        try:
            return transform(sg_value)
        except Exception:
            return None
    if isinstance(sg_value, dict):
        return sg_value.get("name") or sg_value.get("id")
    if isinstance(sg_value, list):
        out = []
        for it in sg_value:
            if isinstance(it, dict):
                out.append(it.get("name") or str(it.get("id")))
            else:
                out.append(str(it))
        return ", ".join(out)
    return sg_value

# ---------------------------
# Flask endpoint
# ---------------------------

@app.route("/send_plates", methods=["POST", "GET"])
def index():
    # --- Per-request debug flag ---
    debug_mode = request.args.get("debug") == "1"

    # --- Local log function scoped to this request ---
    def log(*args, **kwargs):
        if debug_mode:
            print(*args, **kwargs)

    try:
        # parse ids
        entity_type = request.values.get("entity_type", "Element")
        ids = None

        # JSON body?
        if request.is_json:
            body = request.get_json(silent=True) or {}
            ids = body.get("entity_ids") or body.get("ids")
            if isinstance(ids, str):
                ids = [int(x) for x in ids.split(",") if x.strip()]
        else:
            ids_raw = request.values.get("ids")
            if ids_raw:
                ids = [int(x) for x in ids_raw.split(",") if x.strip()]

        if not ids:
            return jsonify({
                "ok": False,
                "error": "No entity IDs provided. Use JSON {entity_ids: [...] } or form/query ids=1,2,3"
            }), 400

        # Connect to ShotGrid
        sg = get_shotgun()
        fields = build_fields_to_query()
        log("Querying ShotGrid fields:", fields)
        filters = [["id", "in", ids]]
        sg_results = sg.find(entity_type, filters, fields)
        log("Found", len(sg_results), "results")

        # --- DEBUG: show actual SG values for each element ---
        log("SG element fields for debug:")
        for e in sg_results:
            log(json.dumps(e, indent=2))

        # --- Optional: show mapping -> FMP ---
        log("Preview of field mapping for each element:")
        for e in sg_results:
            field_preview = {}
            for sg_key, fmp_field in FIELD_MAP.items():
                if sg_key == 'sg_latest_version':
                    latest = e.get('latest_version')
                    field_preview[fmp_field] = latest.get('code') if latest else None
                elif sg_key == 'shot':
                    shot_ref = e.get('shot')
                    field_preview[fmp_field] = shot_ref.get('id') if shot_ref else None
                else:
                    val = sg_value_to_fmp_value(sg_key, e.get(sg_key))
                    field_preview[fmp_field] = val
            log(json.dumps({"sg_id": e.get("id"), "mapped_fields": field_preview}, indent=2))

        # ---- Build FileMaker records ----
        records_to_create = []
        created_meta = []
        skipped_count = 0

        for ent in sg_results:
            fieldData = {}

            for sg_key, fmp_field in FIELD_MAP.items():
                if sg_key == 'sg_latest_version':
                    latest = ent.get('latest_version')
                    if latest:
                        fieldData[fmp_field] = latest.get('code') or latest.get('id')
                    continue

                if sg_key == 'shot':
                    shot_ref = ent.get('shot')
                    if shot_ref:
                        fieldData[fmp_field] = shot_ref.get('id')
                    continue

                sg_val = ent.get(sg_key)
                value = sg_value_to_fmp_value(sg_key, sg_val)
                if value is not None:
                    fieldData[fmp_field] = value

            # Skip if no data
            if not fieldData:
                skipped_count += 1
                created_meta.append({
                    "sg_id": ent.get("id"),
                    "status": "skipped",
                    "reason": "no mapped fields present"
                })
                continue

            records_to_create.append({"fieldData": fieldData})
            created_meta.append({
                "sg_id": ent.get("id"),
                "status": "queued",
                "fields": list(fieldData.keys())
            })

        # Debug preview
        if debug_mode:
            debug_payload = json.dumps(records_to_create, indent=2)
            log("DEBUG: Records about to be sent to FileMaker:\n", debug_payload)
            return Response(
                "<h2>DEBUG: Records about to be sent to FileMaker</h2>"
                f"<pre>{debug_payload}</pre>",
                mimetype="text/html"
            )

        # --- Authenticate and send to FileMaker ---
        token = None
        try:
            token = fm_get_token()
            log("Obtained FMP token:", token)
            r = fm_create_records(token, records_to_create)
            if r.status_code not in (200, 201):
                try:
                    body = r.json()
                except Exception:
                    body = r.text
                raise RuntimeError(f"FileMaker create failed {r.status_code}: {body}")

            resp_json = r.json()
            fm_created = len(resp_json.get("response", {}).get("data", []))

            result = {
                "ok": True,
                "requested": len(records_to_create),
                "created": fm_created,
                "skipped": skipped_count,
                "shotgrid_requested": len(sg_results),
                "details": created_meta,
                "fmp_response": resp_json.get("response", {})
            }
            return jsonify(result)

        finally:
            if token:
                try:
                    fm_close_session(token)
                    log("Closed FMP session.")
                except Exception:
                    pass

    except Exception as exc:
        log("Exception:", traceback.format_exc())
        return jsonify({
            "ok": False,
            "error": str(exc),
            "trace": traceback.format_exc()
        }), 500

if __name__ == "__main__":
    DEBUG = True
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=DEBUG)
