import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid
import time as time_module

SONG_API = "http://hron.fei.tuke.sk:8152/song"
LISTENERS_WS = "ws://hron.fei.tuke.sk:8152/ws/listeners"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{' ' * (8 - len(radio_name))}{session_part} {text}")

def is_valid_song(data):
    # Presne keys, nič navyše ani menej!
    required = {"song", "artist", "start_time"}
    return (
        isinstance(data, dict)
        and set(data.keys()) == required
        and all(data.get(k) is not None for k in required)
    )

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    flat = {k: raw.get(k) for k in ("song", "artist", "start_time")}
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = song_obj["song_session_id"]
    return flat

def get_current_song():
    try:
        r = requests.get(SONG_API, timeout=10)
        data = r.json()
        raw_valid = is_valid_song(data)
        session_id = str(uuid.uuid4())
        return {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception as e:
        log_radio_event("VLNA", f"Skladba sa nenašla, resp. API chyba: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def is_valid_listeners(data):
    required = {"listeners", "timestamp"}
    return (
        isinstance(data, dict)
        and set(data.keys()) == required
        and isinstance(data["listeners"], int)
        and isinstance(data["timestamp"], str)
    )

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    flat = {k: raw.get(k) for k in ("listeners", "timestamp")}
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = listener_obj["song_session_id"]
    return flat

async def get_current_listeners(session_id=None):
    try:
        async with websockets.connect(LISTENERS_WS) as ws:
            msg = await asyncio.wait_for(ws.recv(), timeout=15)
            data = json.loads(msg)
            raw_valid = is_valid_listeners(data)
            return {
                "raw": data,
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": raw_valid,
                "song_session_id": session_id
            }
    except Exception as e:
        log_radio_event("VLNA", f"Nepodarilo sa získať poslucháčov: {e}", session_id)
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
