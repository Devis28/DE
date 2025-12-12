# -*- coding: utf-8 -*-
import os
import json
import time
import asyncio
import threading
import logging
from collections import deque
from datetime import datetime, time as dtime

from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from apscheduler.schedulers.background import BackgroundScheduler

# ── tvoje moduly ───────────────────────────────────────────────────────────────
from melody_core import fetch_html, parse_first_row, estimate_listeners, TZ
from scrape_melody import scrape_page  # očakávame list[dict] s title/artist/date/time

# ── konfigurácia ───────────────────────────────────────────────────────────────
DATA_PATH = os.environ.get("OUT_PATH", "/data/playlist.json")
LIMIT = int(os.environ.get("PLAYLIST_LIMIT", "0"))          # 0 = bez limitu
SCRAPE_EVERY_S = int(os.environ.get("SCRAPE_EVERY_S", "120"))
PUSH_INTERVAL_S = int(os.environ.get("PUSH_INTERVAL_S", "10"))  # kadencia /ws/listeners
SONG_REFRESH_S = int(os.environ.get("SONG_REFRESH_S", "60"))    # pravidelný refresh /ws/song

DATE_FMT = os.environ.get("DATE_FMT", "%Y-%m-%d")
TIME_FMT = os.environ.get("TIME_FMT", "%H:%M:%S")

os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
os.makedirs("/data", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("melody")

app = FastAPI(title="Melody playlist service (WS)", version="1.7.6-ws+origin-referer+delivery-log")

# ── I/O (UTF-8, bez skracovania) ──────────────────────────────────────────────
def load_all(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning("load_all failed: %s", e)
        return []

def save_all(path: str, data: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _k(item: dict) -> tuple:
    # dedup podľa (date,time,artist,title)
    return (item.get("date"), item.get("time"), item.get("artist"), item.get("title"))

def _merge_and_save(new_items: list[dict]) -> tuple[int, int, list[dict]]:
    """Vloží len nové záznamy na začiatok; vráti (added, total, to_add)."""
    existing = load_all(DATA_PATH)
    seen = {_k(x) for x in existing}

    for it in new_items:
        it.setdefault("station", "Rádio Melody")

    to_add = [x for x in new_items if _k(x) not in seen]
    if not to_add:
        return 0, len(existing), []

    merged = to_add + existing
    if LIMIT > 0:
        merged = merged[:LIMIT]
    save_all(DATA_PATH, merged)
    return len(to_add), len(merged), to_add

# ── pomocné: aktuálny „now“ riadok + station ──────────────────────────────────
def fetch_now_row() -> tuple[dict | None, str]:
    html = fetch_html()
    soup = BeautifulSoup(html, "html.parser")
    row = parse_first_row(html)
    h1 = soup.select_one("h1.radio_nazov")
    station = h1.get_text(strip=True) if h1 else "Rádio Melody"
    return row, station

# ── audit: zápis connect/disconnect do súboru + metadata ──────────────────────
WS_LOG_PATH = "/data/ws_connections.log"

def _ws_audit(event: str, path: str, ip: str, ua: str, origin: str = "", referer: str = "") -> None:
    """Zapíše jeden riadok auditu vrátane UA/Origin/Referer."""
    try:
        ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts}  {event:<10}  {path:<12}  {ip}  UA={ua}  ORIGIN={origin}  REF={referer}\n"
        with open(WS_LOG_PATH, "a", encoding="utf-8-sig", newline="\n") as f:
            f.write(line)
    except Exception as e:
        log.warning("ws_audit failed: %s", e)

def _real_ip_from_headers(ws: WebSocket) -> tuple[str, dict]:
    """
    Vráti (best_ip, raw_forwarded_headers).
    Preferuje: Fly-Client-IP > CF-Connecting-IP > X-Real-IP > prvá z X-Forwarded-For > Forwarded
    Fallback je ws.client.host.
    """
    h = ws.headers
    raw = {}
    for k in ("fly-client-ip", "cf-connecting-ip", "x-real-ip", "x-forwarded-for", "forwarded"):
        v = h.get(k)
        if v:
            raw[k] = v

    for key in ("fly-client-ip", "cf-connecting-ip", "x-real-ip"):
        v = h.get(key)
        if v:
            return v.strip(), raw

    xff = h.get("x-forwarded-for")
    if xff:
        parts = [p.strip() for p in xff.split(",") if p.strip()]
        if parts:
            return parts[0], raw

    fwd = h.get("forwarded")
    if fwd:
        import re
        m = re.search(r'for="?(\[?[A-Za-z0-9\.:]+]?)"?', fwd)
        if m:
            ip = m.group(1).strip('"').strip("[]")
            if ip:
                return ip, raw

    ip = ws.client.host if getattr(ws, "client", None) else "unknown"
    return ip, raw

def _client_meta(ws: WebSocket, path: str) -> dict:
    ip, forwarded_hdrs = _real_ip_from_headers(ws)
    h = ws.headers
    origin = h.get("origin") or h.get("sec-websocket-origin", "")
    referer = h.get("referer", "")
    ua = h.get("user-agent", "")
    return {
        "ip": ip,
        "ua": ua,
        "origin": origin,
        "referer": referer,
        "path": path,
        "connected_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "forwarded": forwarded_hdrs,
    }

# ── WebSocket broadcast a evidencie klientov ───────────────────────────────────
_ws_clients_listeners: set[WebSocket] = set()
_ws_clients_song: set[WebSocket] = set()
_ws_info_listeners: dict[WebSocket, dict] = {}
_ws_info_song: dict[WebSocket, dict] = {}

ASYNC_LOOP: asyncio.AbstractEventLoop | None = None

async def _ws_send_many(
    clients: set[WebSocket],
    payload: dict,
    info_map: dict | None = None,
    path: str = "",
    kind: str = "",
):
    dead: list[WebSocket] = []
    for ws in list(clients):
        meta = (info_map or {}).get(ws, {}) if info_map is not None else {}
        try:
            await ws.send_json(payload)
            # ---- KONZOLový ZÁZNAM ÚSPEŠNÉHO DORUČENIA -----------------------
            # čo presne bolo doručené:
            what = ""
            if kind == "listeners":
                what = f"listeners={payload.get('listeners')}"
            elif kind == "song":
                what = f"{payload.get('artist','?')} - {payload.get('title','?')} [{payload.get('time','')}]"
            ts_payload = payload.get("last_update", "")
            log.info(
                "DELIVERED %s to ip=%s origin=%s ref=%s ua=%s at=%s (%s)",
                kind or path,
                meta.get("ip", "unknown"),
                meta.get("origin", ""),
                meta.get("referer", ""),
                (meta.get("ua", "") or "")[:160],
                ts_payload,
                what,
            )
            # -----------------------------------------------------------------
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)
        meta = (info_map or {}).pop(ws, None)
        if meta:
            _ws_audit(
                "disconnect",
                path or meta.get("path", "?"),
                meta.get("ip", "unknown"),
                meta.get("ua", ""),
                meta.get("origin", ""),
                meta.get("referer", ""),
            )

def ws_send_listeners(payload: dict) -> None:
    if ASYNC_LOOP and _ws_clients_listeners:
        asyncio.run_coroutine_threadsafe(
            _ws_send_many(_ws_clients_listeners, payload, _ws_info_listeners, "/ws/listeners", kind="listeners"),
            ASYNC_LOOP,
        )

def ws_send_song(payload: dict) -> None:
    if ASYNC_LOOP and _ws_clients_song:
        asyncio.run_coroutine_threadsafe(
            _ws_send_many(_ws_clients_song, payload, _ws_info_song, "/ws/song", kind="song"),
            ASYNC_LOOP,
        )

# ── WebSocket endpointy ───────────────────────────────────────────────────────
@app.websocket("/ws/listeners")
async def ws_listeners(ws: WebSocket):
    await ws.accept()
    _ws_clients_listeners.add(ws)
    meta = _client_meta(ws, "/ws/listeners")
    _ws_info_listeners[ws] = meta
    _ws_audit("connect", meta["path"], meta["ip"], meta["ua"], meta.get("origin",""), meta.get("referer",""))
    log.info("WS /listeners connected (clients=%d)", len(_ws_clients_listeners))
    try:
        while True:
            await asyncio.sleep(3600)  # drž spojenie
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients_listeners.discard(ws)
        _ws_info_listeners.pop(ws, None)
        _ws_audit("disconnect", meta["path"], meta["ip"], meta["ua"], meta.get("origin",""), meta.get("referer",""))
        log.info("WS /listeners disconnected (clients=%d)", len(_ws_clients_listeners))

@app.websocket("/ws/song")
async def ws_song(ws: WebSocket):
    await ws.accept()
    _ws_clients_song.add(ws)
    meta = _client_meta(ws, "/ws/song")
    _ws_info_song[ws] = meta
    _ws_audit("connect", meta["path"], meta["ip"], meta["ua"], meta.get("origin",""), meta.get("referer",""))
    log.info("WS /song connected (clients=%d)", len(_ws_clients_song))

    # pošli aktuálnu skladbu hneď pri pripojení
    row, station = fetch_now_row()
    if row:
        try:
            await ws.send_json({
                "station": station,
                "title": row["title"],
                "artist": row["artist"],
                "date": row["date"],   # formát z webu (napr. 18.10.2025)
                "time": row["time"],
                "last_update": datetime.now(TZ).strftime("%d.%m.%Y %H:%M:%S"),
            })
        except Exception:
            pass

    try:
        while True:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients_song.discard(ws)
        _ws_info_song.pop(ws, None)
        _ws_audit("disconnect", meta["path"], meta["ip"], meta["ua"], meta.get("origin",""), meta.get("referer",""))
        log.info("WS /song disconnected (clients=%d)", len(_ws_clients_song))

# ── Scrape + zápis ────────────────────────────────────────────────────────────
def scrape_once():
    try:
        items = scrape_page()
        if not isinstance(items, list):
            raise RuntimeError("scrape_page() nevrátil list")
        added, total, _to_add = _merge_and_save(items)
        log.info("scrape_once: %s", {"added": added, "total": total})
        return {"added": added, "total": total}
    except Exception as e:
        log.exception("scrape_once failed")
        return {"error": f"{type(e).__name__}: {e}"}

# ── HTTP endpointy ────────────────────────────────────────────────────────────
@app.get("/song")
def now():
    row, station = fetch_now_row()
    if not row:
        return JSONResponse({"error": "Nepodarilo sa získať aktuálnu skladbu."}, status_code=502)
    return {
        "station": station,
        "title": row["title"],
        "artist": row["artist"],
        "date": row["date"],
        "time": row["time"],
        "last_update": datetime.now(TZ).strftime("%d.%m.%Y %H:%M:%S"),
    }

@app.get("/listeners")
def listeners_now():
    now_dt = datetime.now(TZ)
    seed_key = f"api|listeners|{now_dt.strftime('%F %T')}"
    listeners = estimate_listeners(now_dt, seed_key=seed_key)
    return {"last_update": now_dt.strftime("%d.%m.%Y %H:%M:%S"), "listeners": listeners}

@app.get("/playlist.json")
def playlist_file():
    if not os.path.exists(DATA_PATH):
        return JSONResponse([], status_code=200)
    return FileResponse(DATA_PATH, media_type="application/json")

@app.post("/scrape-now")
def scrape_now():
    return scrape_once()

@app.get("/healthz")
@app.get("/")
def health():
    return PlainTextResponse("ok")

# ── Verejné: ws log (celý súbor) a štatistiky ─────────────────────────────────
@app.get("/data/ws_connections.log", response_class=PlainTextResponse)
def get_ws_log():
    if not os.path.exists(WS_LOG_PATH):
        return PlainTextResponse("(empty)\n", status_code=200)
    return FileResponse(WS_LOG_PATH, media_type="text/plain; charset=utf-8-sig", filename="ws_connections.log")

@app.get("/ws/stats")
def ws_stats():
    def _pack(items: dict[WebSocket, dict]):
        return [
            {
                "ip": meta.get("ip", "unknown"),
                "ua": meta.get("ua", ""),
                "origin": meta.get("origin", ""),
                "referer": meta.get("referer", ""),
                "connected_at": meta.get("connected_at", ""),
                "path": meta.get("path", ""),
            }
            for meta in items.values()
        ]
    listeners = _pack(_ws_info_listeners)
    song = _pack(_ws_info_song)
    return {
        "total": len(listeners) + len(song),
        "listeners": {"count": len(listeners), "clients": listeners},
        "song": {"count": len(song), "clients": song},
    }

# ── Scheduler + WS ticker ─────────────────────────────────────────────────────
scheduler: BackgroundScheduler | None = None
_last_song_key: str | None = None
_last_song_sent_ts: float = 0.0

@app.on_event("startup")
async def _on_startup():
    global scheduler, ASYNC_LOOP
    ASYNC_LOOP = asyncio.get_running_loop()

    if scheduler is None:
        scheduler = BackgroundScheduler(timezone=str(TZ))
        scheduler.add_job(
            scrape_once, "interval",
            seconds=SCRAPE_EVERY_S, id="scrape",
            max_instances=1, coalesce=True, replace_existing=True,
        )
        scheduler.start()
        log.info("Scheduler started: every %ss", SCRAPE_EVERY_S)

    # WS ticker: každých PUSH_INTERVAL_S pošli listeners; song pri zmene/refreshi
    def _ws_ticker():
        global _last_song_key, _last_song_sent_ts
        while True:
            time.sleep(PUSH_INTERVAL_S)
            try:
                row, station = fetch_now_row()
                if not row:
                    continue

                # LISTENERS – „teraz“
                song_key = f'{row["artist"]}|{row["title"]}|{row["date"]}|{row["time"]}'
                now_dt = datetime.now(TZ)
                listeners = estimate_listeners(now_dt, seed_key=song_key)
                ws_send_listeners({
                    "last_update": now_dt.strftime("%d.%m.%Y %H:%M:%S"),
                    "listeners": listeners,
                })

                # SONG – pri zmene alebo po refresh intervale
                now_ts = time.time()
                if (song_key != _last_song_key) or (now_ts - _last_song_sent_ts >= SONG_REFRESH_S):
                    _last_song_key = song_key
                    _last_song_sent_ts = now_ts
                    ws_send_song({
                        "station": station,
                        "title": row["title"],
                        "artist": row["artist"],
                        "date": row["date"],
                        "time": row["time"],
                        "last_update": now_dt.strftime("%d.%m.%Y %H:%M:%S"),
                    })
            except Exception as e:
                log.warning("WS ticker error: %s", e)

    threading.Thread(target=_ws_ticker, daemon=True).start()
    log.info(
        "WS ticker started (listeners every ~%ss; song refresh every ~%ss)",
        PUSH_INTERVAL_S, SONG_REFRESH_S,
    )
