# FMP_plate_pusher
#!/usr/bin/env python3
"""
Element Exporter AMI - Push selected Element (Plate) data from ShotGrid to FileMaker.

How it works:
- ShotGrid AMI / Button should POST JSON like:
    { "entity_type": "Element", "entity_ids": [1234, 2345, ...] }
  or send form data `ids=1234,2345,...`.

Environment variables required:
- SG_URL
- SG_SCRIPT_NAME
- SG_SCRIPT_KEY

- FMP_BASE_URL         (e.g. https://fm.example.com)
- FMP_DATABASE         (FileMaker database name)
- FMP_LAYOUT           (FileMaker layout to use for creating records)
- FMP_USER
- FMP_PASSWORD

Optional:
- DEBUG=true           (prints debug logs)
- RETURN_HTML=yes      (if you want the endpoint to return HTML when ?html=1)

Notes about FileMaker Data API:
- This script expects FileMaker Data API vLatest endpoints:
  /fmi/data/vLatest/databases/{db}/sessions  -> POST { "username": "...", "password": "..." }
  /fmi/data/vLatest/databases/{db}/layouts/{layout}/records -> POST body { "records": [ { "fieldData": {...} } ] }
  Adjust the endpoints if your server/version differs.
"""
