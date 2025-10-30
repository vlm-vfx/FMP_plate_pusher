import os
import json
import requests
from flask import Flask, request, jsonify
from base64 import b64encode
from shotgun_api3 import Shotgun

app = Flask(__name__)

# ---------------------------
# CONFIGURATION
# ---------------------------

SG_URL = os.environ.get("SG_URL")
SG_SCRIPT_NAME = os.environ.get("SG_SCRIPT_NAME")
SG_SCRIPT_KEY = os.environ.get("SG_SCRIPT_KEY")

FMP_BASE_URL = os.environ.get("FMP_BASE_URL")
FMP_DATABASE = os.environ.get("FMP_DATABASE")
FMP_LAYOUT = os.environ.get("FMP_LAYOUT")
FMP_USER = os.environ.get("FMP_USER")
FMP_PASSWORD = os.environ.get("FMP_PASSWORD")

# ---------------------------
# HELPERS
# ---------------------------

def get_sg_connection():
    """Return authenticated ShotGrid API connection."""
    if not all([SG_URL, SG_SCRIPT_NAME, SG_SCRIPT_KEY]):
        raise RuntimeError("Missing ShotGrid environment credentials.")
    return Shotgun(SG_URL, SG_SCRIPT_NAME, SG_SCRIPT_KEY)


def fm_get_token():
    """Authenticate with FileMaker Data API and return session token."""
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


def fm_close_session(token):
    """Close FileMaker session (cleanup)."""
    try:
        url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/sessions/{token}"
        requests.delete(url, headers={"Authorization": f"Bearer {token}"})
    except Exception:
        pass


# ---------------------------
# MAIN ENDPOINT
# ---------------------------

@app.route("/send_plates", methods=["POST", "GET"])
def send_plates():
    # Grab parameters from query string (GET) or form/json body (POST)
    if request.method == "POST":
        entity_type = request.args.get("entity_type") or request.form.get("entity_type") or "Element"
        selected_ids_raw = request.args.get("selected_ids") or request.form.get("selected_ids") or ""
        debug_flag = (request.args.get("debug") or request.form.get("debug") or "").lower() in ("1", "true", "yes")
    else:  # GET
        entity_type = request.args.get("entity_type", "Element")
        selected_ids_raw = request.args.get("selected_ids", "")
        debug_flag = request.args.get("debug", "").lower() in ("1", "true", "yes")

    if debug_flag:
        print("🟡 DEBUG MODE ENABLED")

    try:
        selected_ids = [int(x) for x in selected_ids_raw.split(",") if x.strip().isdigit()]
    except Exception:
        return jsonify({"error": "Invalid selected_ids"}), 400

    if not selected_ids:
        return jsonify({"error": "No valid IDs provided"}), 400

    # --- now your normal SG query and FMP sending logic ---
    sg = get_sg_connection()

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
    for el in elements:
        plate_name = el.get("sg_latest_version", {}).get("name")
        foreign_key = el.get("shot", {}).get("id")
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
        fm_field_data = {k: v for k, v in mapped_fields.items() if v not in (None, "", [])}
        fm_records.append({"fieldData": fm_field_data})

    # --- SEND TO FILEMAKER ---
    try:
        token = fm_get_token()
        url = f"{FMP_BASE_URL}/fmi/data/vLatest/databases/{FMP_DATABASE}/layouts/{FMP_LAYOUT}/records"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = requests.post(url, headers=headers, json={"records": fm_records})
        result = r.json()
    except Exception as e:
        return jsonify({"error": f"Failed to send to FileMaker: {e}"}), 500

    return jsonify({
        "message": f"✅ Sent {len(fm_records)} records to FileMaker.",
        "records": fm_records,
        "filemaker_response": result if debug_flag else "hidden (debug off)"
    })

# ---------------------------
# MAIN
# ---------------------------

if __name__ == "__main__":
    DEBUG = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=DEBUG)
