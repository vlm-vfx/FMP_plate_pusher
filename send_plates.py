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

@app.route("/send_plates", methods=["POST"])
def send_plates():
    sg = get_sg_connection()

    # --- PARAMETERS ---
    entity_type = request.args.get("entity_type", "Element")
    selected_ids = request.args.get("selected_ids", "")
    debug_flag = request.args.get("debug", "").lower() in ("1", "true", "yes")
    if debug_flag:
        print("🟡 DEBUG MODE ENABLED")

    try:
        selected_ids = [int(x) for x in selected_ids.split(",") if x.strip().isdigit()]
    except Exception:
        return jsonify({"error": "Invalid selected_ids"}), 400

    if not selected_ids:
        return jsonify({"error": "No valid IDs provided"}), 400

    # --- SG QUERY ---
    fields = [
        "id",
        "sg_latest_version",
        "sg_latest_version.Code",
        "sg_slate",
        "sg_camera_file_name",
        "sg_source_in",
        "sg_source_out",
        "sg_turnover",
        "sg_head_in",
        "sg_cut_in",
        "sg_cut_out",
        "sg_tail_out",
        "sg_lut",
        "description",
        "shot",
        "shot.code",
    ]
    if debug_flag:
        print("Querying ShotGrid fields:", fields)

    elements = sg.find(entity_type, [["id", "in", selected_ids]], fields)

    if debug_flag:
        print(f"Found {len(elements)} results")
        print("SG element fields for debug:")
        for el in elements:
            print(json.dumps(el, indent=2, default=str))

    # --- MAP TO FILEMAKER ---
    fm_records = []
    preview = []

    for el in elements:
        plate_name = (
            el["sg_latest_version"].get("name")
            if el.get("sg_latest_version")
            else None
        )
        foreign_key = el["shot"]["id"] if el.get("shot") else None

        mapped_fields = {
            "Plate Name": plate_name,
            "Slate": el.get("sg_slate"),
            "Source File Name": el.get("sg_camera_file_name"),
            "Timecode In": el.get("sg_source_in"),
            "Timecode Out": el.get("sg_source_out"),
            "Turnover Package": el.get("sg_turnover"),
            "Head In": el.get("sg_head_in"),
            "Cut In": el.get("sg_cut_in"),
            "Cut Out": el.get("sg_cut_out"),
            "Tail Out": el.get("sg_tail_out"),
            "LUT": el.get("sg_lut"),
            "Notes": el.get("description"),
            "ForeignKey": foreign_key,
        }

        preview.append({"sg_id": el["id"], "mapped_fields": mapped_fields})

        # Only send non-null values to FileMaker
        fm_field_data = {k: v for k, v in mapped_fields.items() if v not in (None, "", [])}
        fm_records.append({"fieldData": fm_field_data})

    if debug_flag:
        print("Preview of field mapping for each element:")
        for p in preview:
            print(json.dumps(p, indent=2))
        print("DEBUG: Records about to be sent to FileMaker:\n", json.dumps(fm_records, indent=2))

    # --- SEND TO FILEMAKER ---
    try:
        token = fm_get_token()
        payload = {"data": fm_records}

        if debug_flag:
            print("Sending payload to FileMaker:", json.dumps(payload, indent=2))

        url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/layouts/{FMP_LAYOUT}/records"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = requests.post(url, headers=headers, json=payload)
        result = r.json()

        if debug_flag:
            print("FileMaker response:", json.dumps(result, indent=2))

    except Exception as e:
        return jsonify({"error": f"Failed to send to FileMaker: {e}"}), 500

    return jsonify({
        "message": f"✅ Sent {len(fm_records)} records to FileMaker.",
        "records": fm_records,
        "filemaker_response": result if debug_flag else "hidden (debug off)"
    })


if __name__ == "__main__":
    DEBUG = True
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=DEBUG)
