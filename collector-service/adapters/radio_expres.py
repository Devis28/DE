import threading
import requests
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request

SONG_FILE = "/tmp/expres_last_song.json"
LISTENERS_API = "http://147.232.205.56:5010/api/current_listeners"

app = Flask(__name__)
latest_song = {"data": {}, "timestamp": None, "raw_valid": False, "song_session_id": None}

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{' ' * (8 - len(radio_name))}{session_part} {text}")

@app.route("/expres_webhook", methods=["POST"])
def expres_webhook():
    raw = request.json
    session_id = str(uuid.uuid4())
    entry = {
        "song": raw.get("song"),
        "artists": raw.get("artists", []),
        "isrc": raw.get("isrc"),
        "start_time": raw.get("start_time"),
        "radio": raw.get("radio"),
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": set(raw.keys()) == {"song", "artists", "isrc", "start_time", "radio"},
        "song_session_id": session_id
    }
    global latest_song
    latest_song = entry
    with open(SONG_FILE, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)
    log_radio_event("EXPRES", f"Prijatý webhook song: {raw.get('song')} | {raw.get('artists')}", session_id)
    return "OK"

def get_current_song():
    try:
        with open(SONG_FILE, encoding="utf-8") as f:
            entry = json.load(f)
        # validácia kľúčov (EXACT match)
        entry["raw_valid"] = set(entry.keys()) >= {"song", "artists", "isrc", "start_time", "radio"} and \
                             set(list(entry.keys())) == {"song", "artists", "isrc", "start_time", "radio", "recorded_at", "raw_valid", "song_session_id"}
        if not entry["raw_valid"]:
            log_radio_event("EXPRES", f"Chybný formát skladby: {entry}")
        return entry
    except Exception as e:
        log_radio_event("EXPRES", f"Chyba pri čítaní súboru so skladbou: {e}")
        return {
            "song": None,
            "artists": [],
            "isrc": None,
            "start_time": None,
            "radio": None,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def get_current_listeners(session_id=None):
    try:
        log_radio_event("EXPRES", f"Pokúšam sa pripojiť na: {LISTENERS_API}", session_id)
        r = requests.get(LISTENERS_API, timeout=30)
        log_radio_event("EXPRES", f"HTTP status: {r.status_code}", session_id)
        if r.status_code == 200:
            data = r.json()
            # validácia presná! len klúče timestamp, listeners, radio
            valid_keys = {"timestamp", "listeners", "radio"}
            raw_valid = set(data.keys()) == valid_keys and \
                        isinstance(data["listeners"], int) and \
                        isinstance(data["timestamp"], str) and \
                        isinstance(data["radio"], str)
            if not raw_valid:
                log_radio_event("EXPRES", f"API listeners payload invalid: {data}", session_id)
            return {
                "raw": data,
                "recorded_at": data.get("timestamp") or datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": raw_valid,
                "song_session_id": session_id
            }
        else:
            log_radio_event("EXPRES", f"Chybný HTTP status: {r.status_code}", session_id)
    except Exception as e:
        log_radio_event("EXPRES", f"Nepodarilo sa získať poslucháčov: {e}", session_id)
    return {
        "raw": {},
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": False,
        "song_session_id": session_id
    }

def flatten_song(entry):
    flat = {
        "song": entry.get("song"),
        "artists": entry.get("artists", []),
        "isrc": entry.get("isrc"),
        "start_time": entry.get("start_time"),
        "radio": entry.get("radio"),
        "recorded_at": entry.get("recorded_at"),
        "raw_valid": entry.get("raw_valid"),
        "song_session_id": entry.get("song_session_id")
    }
    return flat

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    flat = {"timestamp": raw.get("timestamp"),
            "listeners": raw.get("listeners"),
            "radio": raw.get("radio")}
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = listener_obj["song_session_id"]
    return flat

def start_expres_webhook():
    def run_flask():
        log_radio_event("EXPRES", "Spúštam Flask webhook server na porte 8001")
        app.run(host='0.0.0.0', port=8001, debug=False)
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
