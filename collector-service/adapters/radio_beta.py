import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    radio_fmt = f"{radio_name:}"
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_fmt}]{' ' * (8 - len(radio_name))}{session_part} {text}")

def is_valid_song(data):
    wanted = {"radio", "interpreters", "title", "start_time", "timestamp"}
    # valid len ak presne tieto (playing case)
    return (
        isinstance(data, dict)
        and set(data.keys()) == wanted
        and all(data[k] is not None for k in wanted)
    )

def is_valid_song_idle(data):
    # valid len ak presne: radio, is_playing, message, timestamp (not playing case)
    wanted = {"radio", "is_playing", "message", "timestamp"}
    return (
        isinstance(data, dict)
        and set(data.keys()) == wanted
        and data.get("is_playing") is False
        and all(k in data for k in wanted)
    )

def is_valid_listeners(data):
    # valid len ak presne: listeners, timestamp
    return (
        isinstance(data, dict)
        and set(data.keys()) == {"listeners", "timestamp"}
        and isinstance(data["listeners"], int)
        and isinstance(data["timestamp"], str)
    )

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    flat = dict(raw)
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = song_obj["song_session_id"]
    return flat

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    flat = dict(raw)
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = listener_obj["song_session_id"]
    return flat

def get_current_song():
    try:
        r = requests.get(SONG_API, timeout=10)
        data = r.json()
        session_id = str(uuid.uuid4())
        rec_at = datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")
        if is_valid_song(data):
            return {
                "raw": data,
                "recorded_at": rec_at,
                "raw_valid": True,
                "song_session_id": session_id
            }
        elif is_valid_song_idle(data):
            return {
                "raw": data,
                "recorded_at": rec_at,
                "raw_valid": True,
                "song_session_id": session_id
            }
        else:
            log_radio_event("BETA", f"Song nenašiel požadované polia: {data}", session_id)
            return {
                "raw": data,
                "recorded_at": rec_at,
                "raw_valid": False,
                "song_session_id": session_id
            }
    except Exception as e:
        log_radio_event("BETA", f"Chyba pri získavaní skladby: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def get_current_listeners(session_id=None):
    try:
        async with websockets.connect(LISTENERS_WS) as websocket:
            msg = await asyncio.wait_for(websocket.recv(), timeout=30)
            data = json.loads(msg)
            raw_valid = is_valid_listeners(data)
            if not raw_valid:
                log_radio_event("BETA", f"Neplatná štruktúra listeners: {data}", session_id)
            return {
                "raw": data,
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": raw_valid,
                "song_session_id": session_id
            }
    except Exception as e:
        log_radio_event("BETA", f"Nepodarilo sa získať poslucháčov: {e}", session_id)
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
