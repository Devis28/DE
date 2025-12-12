import threading
import requests
import asyncio
from fastapi import FastAPI, Request
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "http://147.232.40.154:8000/current"

last_listeners_payload = {}
last_lock = threading.Lock()

app = FastAPI()

@app.post("/callback")
@app.post("/callback-jazz")
async def callback(req: Request):
    data = await req.json()
    now = datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")
    required = {"timestamp", "listeners", "radio"}
    valid = isinstance(data, dict) and set(data.keys()) == required and isinstance(data["listeners"], int)
    payload = {
        "raw": data,
        "recorded_at": now,
        "raw_valid": valid,
        "song_session_id": None
    }
    with last_lock:
        last_listeners_payload.clear()
        last_listeners_payload.update(payload)
    print(f"[{now}] [JAZZ] Webhook: {data}")
    return {"status": "ok"}

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{' ' * (8 - len(radio_name))}{session_part} {text}")

def is_valid_song(data):
    song = data.get("song") if isinstance(data, dict) else None
    required = {"play_date", "play_time", "artist", "title"}
    return isinstance(song, dict) and set(song.keys()) == required

def flatten_song(song_obj):
    raw = song_obj.get("raw", {})
    song = raw.get("song", {}) if isinstance(raw.get("song", {}), dict) else {}
    session_id = song_obj["song_session_id"]
    flat = {k: song.get(k) for k in ["play_date", "play_time", "artist", "title"]}
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
        song = data.get("song", {}) if isinstance(data, dict) else {}
        if not raw_valid:
            log_radio_event("JAZZ", f"Song nenašiel požadované polia: {data}", session_id)
        return {
            "raw": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id,
            "title": song.get("title"),
            "artist": song.get("artist"),
        }
    except Exception as e:
        log_radio_event("JAZZ", f"Chyba pri získavaní songu: {e}")
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

def flatten_listener(listener_obj):
    raw = listener_obj.get("raw", {})
    session_id = listener_obj["song_session_id"]
    flat = {k: raw.get(k) for k in ["timestamp", "listeners", "radio"]}
    flat["recorded_at"] = listener_obj["recorded_at"]
    flat["raw_valid"] = listener_obj["raw_valid"]
    flat["song_session_id"] = session_id
    return flat

async def get_current_listeners(session_id=None):
    global last_listeners_payload
    with last_lock:
        payload = last_listeners_payload.copy()
    if not payload or not payload.get("raw_valid"):
        log_radio_event("JAZZ", f"Nepodarilo sa načítať poslucháčov, používaj webhook!", session_id)
        return {
            "raw": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }
    payload["song_session_id"] = session_id
    return payload

async def main_jazz_worker():
    previous_key = None
    session_id = None
    while True:
        current_song = get_current_song()
        title = current_song.get("title")
        artist = current_song.get("artist")
        key = (title, artist)
        if not current_song["raw_valid"]:
            log_radio_event("JAZZ", f"Skladba sa nenašla, alebo nesprávne dáta! {current_song.get('raw')}", session_id)
        elif previous_key != key and current_song["raw_valid"]:
            session_id = str(uuid.uuid4())
            previous_key = key
            current_song["song_session_id"] = session_id
            log_radio_event("JAZZ", f"Zachytená skladba: {title} | {artist}", session_id)
        else:
            log_radio_event("JAZZ", f"Skladba nezmenená: {title} | {artist}", session_id)
        listeners_data = await get_current_listeners(session_id)
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event("JAZZ", f"Nepodarilo sa získať poslucháčov alebo nesprávne dáta! {raw_list}", session_id)
        log_radio_event("JAZZ", f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        await asyncio.sleep(30)

@app.on_event("startup")
async def start_worker():
    asyncio.create_task(main_jazz_worker())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("adapters.radio_jazz:app", host="0.0.0.0", port=8002, reload=True)
