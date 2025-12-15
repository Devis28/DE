import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from requests.exceptions import RequestException

import requests

# --------- API KEYS / KONŠTANTY ---------
MUSICBRAINZ_USER_AGENT = os.environ.get("MUSICBRAINZ_USER_AGENT", "")
LASTFM_API_KEY         = os.environ.get("LASTFM_API_KEY", "")
ITUNES_COUNTRY         = os.environ.get("ITUNES_COUNTRY", "sk")
SPOTIFY_CLIENT_ID      = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET  = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
LISTENBRAINZ_API_TOKEN = os.environ.get("LISTENBRAINZ_API_TOKEN", "")

# --------- CESTY K SÚBOROM ---------
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

SILVER_INPUT = os.path.join(
    ROOT_DIR,
    "silver_transform_merged0",
    "silver_merged.json",
)

ENRICH_DIR = os.path.join(ROOT_DIR, "silver_enrich")
ENRICH_OUTPUT = os.path.join(ENRICH_DIR, "silver_enrich.json")

PARTIAL_PATH = os.path.join(ENRICH_DIR, "silver_enrich_partial.json")
STATE_PATH   = os.path.join(ENRICH_DIR, "enrich_state.json")
CHECKPOINT_EVERY = 1000  # uloženie stavu po každom 1000. zázname

# --------- POMOCNÉ FUNKCIE ---------

def ensure_output_dir() -> None:
    os.makedirs(ENRICH_DIR, exist_ok=True)

def normalize_query(title: str, artists: List[str]) -> Tuple[str, str]:
    title_q = title.strip()
    artist_q = ", ".join(a.strip() for a in artists if a.strip())
    return title_q, artist_q

# --------- MUSICBRAINZ ---------

def mb_search_recording(title: str, artist: str) -> Optional[Dict[str, Any]]:
    params = {
        "query": f'recording:"{title}" AND artist:"{artist}"',
        "fmt": "json",
        "limit": 1,
    }
    headers = {"User-Agent": MUSICBRAINZ_USER_AGENT}
    try:
        resp = requests.get(
            "https://musicbrainz.org/ws/2/recording/",
            params=params,
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
    except RequestException:
        return None

    data = resp.json()
    recordings = data.get("recordings") or []
    return recordings[0] if recordings else None

def enrich_from_musicbrainz(title: str, artists: List[str]) -> Dict[str, Optional[Any]]:
    title_q, artist_q = normalize_query(title, artists)
    rec = mb_search_recording(title_q, artist_q)
    if not rec:
        return {"duration": None, "genre": None, "release_year": None}

    duration = rec.get("length")
    release_year = None
    if rec.get("releases"):
        date_str = rec["releases"][0].get("date")
        if date_str:
            year = date_str.split("-")[0]
            if year.isdigit():
                release_year = int(year)

    genre = None
    tags = rec.get("tags") or rec.get("genres") or []
    if tags:
        first = tags[0]
        if isinstance(first, dict):
            genre = first.get("name")
        elif isinstance(first, str):
            genre = first

    return {
        "duration": duration,
        "genre": genre,
        "release_year": release_year,
    }

# --------- LAST.FM ---------

def enrich_from_lastfm(title: str, artists: List[str]) -> Dict[str, Optional[Any]]:
    if not LASTFM_API_KEY:
        return {"duration": None, "genre": None, "release_year": None}

    title_q, artist_q = normalize_query(title, artists)
    params = {
        "method": "track.getInfo",
        "api_key": LASTFM_API_KEY,
        "track": title_q,
        "artist": artist_q,
        "format": "json",
    }
    try:
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
    except RequestException:
        return {"duration": None, "genre": None, "release_year": None}

    data = resp.json()
    track = data.get("track")
    if not track:
        return {"duration": None, "genre": None, "release_year": None}

    duration = None
    if track.get("duration"):
        try:
            duration = int(track["duration"]) * 1000
        except ValueError:
            pass

    genre = None
    tags = track.get("toptags", {}).get("tag") or []
    if tags:
        first = tags[0]
        if isinstance(first, dict):
            genre = first.get("name")

    release_year = None
    wiki = track.get("wiki") or {}
    if "published" in wiki:
        year = wiki["published"].split("-")[0]
        if year.isdigit():
            release_year = int(year)

    return {
        "duration": duration,
        "genre": genre,
        "release_year": release_year,
    }

# --------- ITUNES ---------

def enrich_from_itunes(title: str, artists: List[str]) -> Dict[str, Optional[Any]]:
    title_q, artist_q = normalize_query(title, artists)
    term = f"{title_q} {artist_q}".strip()
    params = {
        "term": term,
        "media": "music",
        "limit": 1,
        "country": ITUNES_COUNTRY,
    }
    resp = requests.get("https://itunes.apple.com/search", params=params, timeout=10)
    if resp.status_code != 200:
        return {"duration": None, "genre": None, "release_year": None}

    results = resp.json().get("results") or []
    if not results:
        return {"duration": None, "genre": None, "release_year": None}

    track = results[0]
    duration_ms = track.get("trackTimeMillis")
    genre = track.get("primaryGenreName")
    release_year = None
    date_str = track.get("releaseDate")
    if date_str:
        year = date_str.split("-")[0]
        if year.isdigit():
            release_year = int(year)

    return {
        "duration": duration_ms,
        "genre": genre,
        "release_year": release_year,
    }

# --------- SPOTIFY ---------

_spotify_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": 0.0}

def get_spotify_token() -> Optional[str]:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None

    now = time.time()
    if _spotify_token_cache["access_token"] and _spotify_token_cache["expires_at"] > now + 60:
        return _spotify_token_cache["access_token"]

    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
        timeout=10,
    )
    if resp.status_code != 200:
        return None

    data = resp.json()
    _spotify_token_cache["access_token"] = data["access_token"]
    _spotify_token_cache["expires_at"] = now + int(data.get("expires_in", 3600))
    return _spotify_token_cache["access_token"]

def spotify_search_track(title: str, artists: List[str]) -> Optional[Dict[str, Any]]:
    token = get_spotify_token()
    if not token:
        return None

    title_q, artist_q = normalize_query(title, artists)
    q_parts = [f'track:"{title_q}"']
    if artist_q:
        q_parts.append(f'artist:"{artist_q}"')
    query = " ".join(q_parts)

    headers = {"Authorization": f"Bearer {token}"}
    params = {"q": query, "type": "track", "limit": 1}
    resp = requests.get("https://api.spotify.com/v1/search", headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        return None

    items = resp.json().get("tracks", {}).get("items") or []
    return items[0] if items else None

def enrich_from_spotify(title: str, artists: List[str]) -> Dict[str, Optional[Any]]:
    track = spotify_search_track(title, artists)
    if not track:
        return {"duration": None, "genre": None, "release_year": None}

    duration_ms = track.get("duration_ms")
    release_year = None
    if track.get("album", {}).get("release_date"):
        year = track["album"]["release_date"].split("-")[0]
        if year.isdigit():
            release_year = int(year)

    return {
        "duration": duration_ms,
        "genre": None,
        "release_year": release_year,
    }

# --------- LISTENBRAINZ ---------

def enrich_from_listenbrainz(title: str, artists: List[str]) -> Dict[str, Optional[Any]]:
    if not LISTENBRAINZ_API_TOKEN:
        return {"duration": None, "genre": None, "release_year": None}

    title_q, artist_q = normalize_query(title, artists)
    params = {"recording_name": title_q, "artist_name": artist_q, "count": 1}
    headers = {"Authorization": f"Token {LISTENBRAINZ_API_TOKEN}"}

    try:
        resp = requests.get(
            "https://api.listenbrainz.org/1/metadata/recording",
            headers=headers,
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
    except RequestException:
        return {"duration": None, "genre": None, "release_year": None}

    payload = resp.json().get("recordings") or []
    if not payload:
        return {"duration": None, "genre": None, "release_year": None}

    rec = payload[0]
    duration = rec.get("length")

    release_year = None
    date_str = rec.get("first_release_date")
    if date_str:
        year = date_str.split("-")[0]
        if year.isdigit():
            release_year = int(year)

    genre = None
    tags = rec.get("tags") or []
    if tags:
        first = tags[0]
        if isinstance(first, dict):
            genre = first.get("name")

    return {
        "duration": duration,
        "genre": genre,
        "release_year": release_year,
    }

# --------- ENRICH LOGIKA PRE JEDEN ZÁZNAM ---------

def merge_enrich(base: Dict[str, Optional[Any]], new: Optional[Dict[str, Optional[Any]]]) -> Dict[str, Optional[Any]]:
    if new is None:
        return base

    result = base.copy()
    for key in ["duration", "genre", "release_year"]:
        if result.get(key) is None and new.get(key) is not None:
            result[key] = new[key]
    return result

def enrich_record(record: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    title = record.get("title") or ""
    artists = record.get("artists") or []
    if isinstance(artists, str):
        artists = [artists]

    meta: Dict[str, Optional[Any]] = {
        "duration": record.get("duration"),
        "genre": record.get("genre"),
        "release_year": record.get("release_year"),
    }

    if meta["duration"] is not None and meta["genre"] is not None and meta["release_year"] is not None:
        return record, True

    meta = merge_enrich(meta, enrich_from_musicbrainz(title, artists))
    if meta["duration"] is not None and meta["genre"] is not None and meta["release_year"] is not None:
        record.update(meta)
        return record, True

    meta = merge_enrich(meta, enrich_from_lastfm(title, artists))
    if meta["duration"] is not None and meta["genre"] is not None and meta["release_year"] is not None:
        record.update(meta)
        return record, True

    meta = merge_enrich(meta, enrich_from_itunes(title, artists))
    if meta["duration"] is not None and meta["genre"] is not None and meta["release_year"] is not None:
        record.update(meta)
        return record, True

    meta = merge_enrich(meta, enrich_from_spotify(title, artists))
    if meta["duration"] is not None and meta["genre"] is not None and meta["release_year"] is not None:
        record.update(meta)
        return record, True

    meta = merge_enrich(meta, enrich_from_listenbrainz(title, artists))

    record.update(meta)
    all_found = meta["duration"] is not None and meta["genre"] is not None and meta["release_year"] is not None
    return record, all_found

# --------- CHECKPOINT FUNKCIE ---------

def load_checkpoint() -> Tuple[int, List[Dict[str, Any]], int]:
    if not os.path.exists(STATE_PATH) or not os.path.exists(PARTIAL_PATH):
        return 0, [], 0

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)
    start_index = state.get("next_index", 0)
    fully_found_count = state.get("fully_found_count", 0)

    with open(PARTIAL_PATH, "r", encoding="utf-8") as f:
        enriched_records = json.load(f)

    return start_index, enriched_records, fully_found_count

def save_checkpoint(next_index: int,
                    enriched_records: List[Dict[str, Any]],
                    fully_found_count: int) -> None:
    ensure_output_dir()
    with open(PARTIAL_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched_records, f, ensure_ascii=False, indent=2)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "next_index": next_index,
                "fully_found_count": fully_found_count,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

# --------- MAIN ---------

def main() -> None:
    start_time = time.time()

    with open(SILVER_INPUT, "r", encoding="utf-8") as f:
        records: List[Dict[str, Any]] = json.load(f)

    total = len(records)

    start_index, enriched_records, fully_found_count = load_checkpoint()
    if start_index > 0:
        print(f"Pokračujem od indexu {start_index} z {total} (už spracované: {len(enriched_records)})")

    enriched_records = enriched_records[:start_index]

    for idx in range(start_index, total):
        rec = records[idx]
        enriched, all_found = enrich_record(rec)
        enriched_records.append(enriched)
        if all_found:
            fully_found_count += 1

        current = idx + 1

        if current % 100 == 0:
            elapsed = time.time() - start_time
            print(
                f"[{current}/{total}] spracovaných záznamov, "
                f"úplne obohatené: {fully_found_count}, "
                f"čas behu: {elapsed:.1f}s"
            )

        if current % CHECKPOINT_EVERY == 0:
            save_checkpoint(current, enriched_records, fully_found_count)
            print(f"Checkpoint uložený pri indexe {current}")

    ensure_output_dir()
    with open(ENRICH_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(enriched_records, f, ensure_ascii=False, indent=2)

    if os.path.exists(STATE_PATH):
        os.remove(STATE_PATH)
    if os.path.exists(PARTIAL_PATH):
        os.remove(PARTIAL_PATH)

    elapsed_total = time.time() - start_time
    print(
        f"Hotovo. Úplne obohatených: {fully_found_count}/{total}, "
        f"celkový čas behu: {elapsed_total:.1f}s"
    )

if __name__ == "__main__":
    main()
