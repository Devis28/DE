import time
import json
import asyncio
import uuid
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from writer import upload_file

from adapters.radio_melody import (
    get_current_song as get_song_melody,
    get_current_listeners as get_listeners_melody,
    flatten_song as flatten_melody_song,
    flatten_listener as flatten_melody_listener
)
from adapters.radio_rock import (
    get_current_song as get_song_rock,
    get_current_listeners as get_listeners_rock,
    flatten_song as flatten_rock_song,
    flatten_listener as flatten_rock_listener
)
from adapters.radio_funradio import (
    get_current_song as get_song_funradio,
    get_current_listeners as get_listeners_funradio,
    flatten_song as flatten_funradio_song,
    flatten_listener as flatten_funradio_listener
)
from adapters.radio_vlna import (
    get_current_song as get_song_vlna,
    get_current_listeners as get_listeners_vlna,
    flatten_song as flatten_vlna_song,
    flatten_listener as flatten_vlna_listener
)
from adapters.radio_beta import (
    get_current_song as get_song_beta,
    get_current_listeners as get_listeners_beta,
    flatten_song as flatten_beta_song,
    flatten_listener as flatten_beta_listener
)
from adapters.radio_expres import (
    get_current_song as get_song_expres,
    get_current_listeners as get_listeners_expres,
    flatten_song as flatten_expres_song,
    flatten_listener as flatten_expres_listener
)
from adapters.radio_jazz import (
    get_current_song as get_song_jazz,
    get_current_listeners as get_listeners_jazz,
    flatten_song as flatten_jazz_song,
    flatten_listener as flatten_jazz_listener,
    log_radio_event,
)

INTERVAL = 30
INTERVAL_VLNA = 40
BATCH_TIME = 7200

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    prefix = f"[{timestamp}] [{radio_name}]{' ' * (8 - len(radio_name))}"
    if session_id:
        prefix += f" [{session_id}]"
    print(prefix + ' ' + text)

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def melody_worker():
    RADIO_NAME = "MELODY"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_key = None
    session_id = None
    while True:
        current_song = get_song_melody()
        raw = current_song.get("raw", {})
        title = raw.get("title")
        artist = raw.get("artist")
        key = (title, artist)
        if not current_song["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatný alebo žiadny song z API! Raw: {raw}", session_id)
        if previous_key != key:
            session_id = str(uuid.uuid4())
            previous_key = key
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(flatten_melody_song(current_song))
        else:
            log_radio_event(RADIO_NAME, f"Skladba nezmenená: {title} | {artist}", session_id)
        listeners_data = asyncio.run(get_listeners_melody(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_melody_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_melody_song.json"
            listeners_path_local = f"{timestamp}_melody_listeners.json"
            song_path_r2 = f"bronze/MELODY/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/MELODY/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL)

def rock_worker():
    RADIO_NAME = "ROCK"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_key = None
    session_id = None
    while True:
        current_song = get_song_rock()
        raw = current_song.get("raw", {})
        song_info = raw.get("song", {})
        title = song_info.get("musicTitle")
        author = song_info.get("musicAuthor")
        key = (title, author)
        if not current_song["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatný alebo žiadny song z API! Raw: {raw}", session_id)
        if previous_key != key and current_song["raw_valid"]:
            session_id = str(uuid.uuid4())
            previous_key = key
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {author}", session_id)
            song_data_batch.append(flatten_rock_song(current_song))
        else:
            log_radio_event(RADIO_NAME, f"Skladba nezmenená: {title} | {author}", session_id)
        listeners_data = asyncio.run(get_listeners_rock(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatná štruktúra listeners: {raw_list}", session_id)
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_rock_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_rock_song.json"
            listeners_path_local = f"{timestamp}_rock_listeners.json"
            song_path_r2 = f"bronze/ROCK/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/ROCK/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL)

def funradio_worker():
    RADIO_NAME = "FUNRADIO"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_author = None
    session_id = None
    while True:
        current_song = get_song_funradio()
        song = current_song["raw"].get("song", {}) if isinstance(current_song.get("raw"), dict) else {}
        title = song.get("musicTitle")
        author = song.get("musicAuthor")
        if not current_song["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatný alebo žiadny song z API! Raw: {song}", session_id)
        elif (previous_title != title or previous_author != author) and title and author:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_author = author
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {author}", session_id)
            song_data_batch.append(flatten_funradio_song(current_song))
        elif title and author:
            log_radio_event(RADIO_NAME, f"Skladba nezmenená: {title} | {author}", session_id)
        listeners_data = asyncio.run(get_listeners_funradio(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatná štruktúra listeners: {raw_list}", session_id)
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_funradio_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_funradio_song.json"
            listeners_path_local = f"{timestamp}_funradio_listeners.json"
            song_path_r2 = f"bronze/FUNRADIO/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/FUNRADIO/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL)

def vlna_worker():
    RADIO_NAME = "VLNA"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    last_song = None
    session_id = None
    while True:
        current_song = get_song_vlna()
        raw = current_song.get("raw", {})
        title = raw.get("song")
        artist = raw.get("artist")
        key = (title, artist)
        if not current_song["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Skladba sa nenašla, alebo nesprávne dáta!", session_id)
        if last_song != key and current_song["raw_valid"]:
            session_id = str(uuid.uuid4())
            last_song = key
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(flatten_vlna_song(current_song))
        listeners_data = asyncio.run(get_listeners_vlna(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Nepodarilo sa získať počet poslucháčov (prázdny alebo zlá štruktúra)!", session_id)
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_vlna_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_vlna_song.json"
            listeners_path_local = f"{timestamp}_vlna_listeners.json"
            song_path_r2 = f"bronze/VLNA/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/VLNA/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL_VLNA)

def beta_worker():
    RADIO_NAME = "BETA"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_interpreters = None
    session_id = None
    while True:
        current_song = get_song_beta()
        raw = current_song.get("raw", {})
        if raw.get("is_playing", True):
            title = raw.get("title")
            interpreters = raw.get("interpreters")
        else:
            title = None
            interpreters = None
        if not current_song["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatný alebo žiadny song z API! Raw: {raw}", session_id)
        elif (title != previous_title or interpreters != previous_interpreters) and title and interpreters:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_interpreters = interpreters
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {interpreters}", session_id)
            song_data_batch.append(flatten_beta_song(current_song))
        elif title and interpreters:
            log_radio_event(RADIO_NAME, f"Skladba nezmenená: {title} | {interpreters}", session_id)
        listeners_data = asyncio.run(get_listeners_beta(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatná štruktúra listeners: {raw_list}", session_id)
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_beta_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_beta_song.json"
            listeners_path_local = f"{timestamp}_beta_listeners.json"
            song_path_r2 = f"bronze/BETA/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/BETA/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL)

def expres_worker():
    RADIO_NAME = "EXPRES"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_song = None
    session_id = None
    while True:
        song = get_song_expres()
        title = song.get("song")
        artist = ", ".join(song.get("artists", []))
        current_song_identifier = f"{title}_{artist}"
        if not song.get("raw_valid"):
            log_radio_event(RADIO_NAME, f"Neplatný alebo žiadny song z API! Raw: {song}", session_id)
        if title and (current_song_identifier != previous_song) and song.get("raw_valid"):
            session_id = song.get("song_session_id", str(uuid.uuid4()))
            previous_song = current_song_identifier
            song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(flatten_expres_song(song))
        else:
            log_radio_event(RADIO_NAME, f"Skladba nezmenená: {title} | {artist}", session_id)
        listeners_data = get_listeners_expres(session_id)
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Neplatná štruktúra listeners: {raw_list}", session_id)
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_expres_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_expres_song.json"
            listeners_path_local = f"{timestamp}_expres_listeners.json"
            song_path_r2 = f"bronze/EXPRES/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/EXPRES/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL)

def jazz_worker():
    RADIO_NAME = "JAZZ"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_key = None
    session_id = None
    while True:
        current_song = get_song_jazz()
        title = current_song.get("title")
        artist = current_song.get("artist")
        key = (title, artist)
        if not current_song["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Skladba sa nenašla, alebo nesprávne dáta! {current_song.get('raw')}", session_id)
        if previous_key != key and current_song["raw_valid"]:
            session_id = str(uuid.uuid4())
            previous_key = key
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(flatten_jazz_song(current_song))
        else:
            log_radio_event(RADIO_NAME, f"Skladba nezmenená: {title} | {artist}", session_id)
        listeners_data = asyncio.run(get_listeners_jazz(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        if not listeners_data["raw_valid"]:
            log_radio_event(RADIO_NAME, f"Nepodarilo sa získať poslucháčov alebo nesprávne dáta! {raw_list}", session_id)
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {raw_list.get('listeners', '?')}", session_id)
        listeners_data_batch.append(flatten_jazz_listener(listeners_data))
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_jazz_song.json"
            listeners_path_local = f"{timestamp}_jazz_listeners.json"
            song_path_r2 = f"bronze/JAZZ/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/JAZZ/listeners/{date_str}/{timestamp}.json"
            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)
            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_radio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)
            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()
        time.sleep(INTERVAL)

def main():
    from adapters.radio_expres import start_expres_webhook
    start_expres_webhook()
    threading.Thread(target=melody_worker, daemon=True).start()
    threading.Thread(target=rock_worker, daemon=True).start()
    threading.Thread(target=funradio_worker, daemon=True).start()
    threading.Thread(target=vlna_worker, daemon=True).start()
    threading.Thread(target=beta_worker, daemon=True).start()
    threading.Thread(target=expres_worker, daemon=True).start()
    threading.Thread(target=jazz_worker, daemon=True).start()
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
