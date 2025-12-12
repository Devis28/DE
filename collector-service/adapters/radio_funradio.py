import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://funradio-server.fly.dev/pull/playing"
LISTENERS_WS = "wss://funradio-server.fly.dev/ws/push/listenership"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def is_valid_song(data):
    required_keys = {"musicAuthor", "musicCover", "musicTitle", "radio", "startTime"}
    return (
        isinstance(data, dict)
        and "song" in data
        and set(data["song"].keys()) == required_keys
    )


def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    flat = {k: raw["song"][k] for k in ("musicAuthor", "musicCover", "musicTitle", "radio", "startTime")} if "song" in raw else {}
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
        log_radio_event("FUNRADIO", f"Skladba nebola získaná: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def is_valid_listeners(data):
    # Presne len listeners key, nič menej ani viac, a musí byť int
    return (
        isinstance(data, dict)
        and set(data.keys()) == {"listeners"}
        and isinstance(data.get("listeners"), int)
    )

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    flat = {"listeners": raw.get("listeners")}
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = listener_obj["song_session_id"]
    return flat

async def get_current_listeners(session_id=None):
    try:
        async with websockets.connect(LISTENERS_WS) as websocket:
            msg = await asyncio.wait_for(websocket.recv(), timeout=20)
            data = json.loads(msg)
            raw_valid = is_valid_listeners(data)
            result = {
                "raw": data,
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": raw_valid,
                "song_session_id": session_id
            }
            if not raw_valid:
                log_radio_event("FUNRADIO", f"Nesprávne alebo chýbajúce údaje o listeners ({data})", session_id)
            return result
    except Exception as e:
        log_radio_event("FUNRADIO", f"Nepodarilo sa získať listeners: {e}", session_id)
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
