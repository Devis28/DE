import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS = "wss://rock-server.fly.dev/ws/push/listenership"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def is_valid_song(data):
    # song musí mať presne tieto atribúty, nič navyše ani menej!
    required_song_keys = {"musicAuthor", "musicCover", "musicTitle", "radio", "startTime"}
    required_top_level = {"last_update", "song"}
    return (
        isinstance(data, dict)
        and set(data.keys()) == required_top_level
        and isinstance(data["song"], dict)
        and set(data["song"].keys()) == required_song_keys
    )

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    session_id = song_obj["song_session_id"]
    flat = {k: raw["song"][k] for k in ["musicAuthor", "musicCover", "musicTitle", "radio", "startTime"]} if "song" in raw and isinstance(raw["song"], dict) else {}
    flat["last_update"] = raw.get("last_update")
    flat["recorded_at"] = song_obj["recorded_at"]
    flat["raw_valid"] = song_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

def get_current_song():
    try:
        r = requests.get(SONG_API, timeout=10)
        data = r.json()
        raw_valid = is_valid_song(data)
        session_id = str(uuid.uuid4())
        if not raw_valid:
            log_radio_event("ROCK", f"Song nenašiel požadované polia: {data}", session_id)
        return {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception as e:
        log_radio_event("ROCK", f"Chyba pri získavaní songu: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def is_valid_listeners(data):
    # Len "listeners" (nič navyše)
    return (
        isinstance(data, dict)
        and set(data.keys()) == {"listeners"}
        and isinstance(data.get("listeners"), int)
    )

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    session_id = listener_obj["song_session_id"]
    flat = {"listeners": raw.get("listeners")}
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

async def get_current_listeners(session_id=None):
    try:
        async with websockets.connect(LISTENERS_WS) as websocket:
            msg = await asyncio.wait_for(websocket.recv(), timeout=20)
            data = json.loads(msg)
            raw_valid = is_valid_listeners(data)
            if not raw_valid:
                log_radio_event("ROCK", f"Neplatná štruktúra listeners: {data}", session_id)
            return {
                "raw": data,
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": raw_valid,
                "song_session_id": session_id
            }
    except Exception as e:
        log_radio_event("ROCK", f"Nepodarilo sa získať listeners: {e}", session_id)
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
