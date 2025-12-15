import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

# --------- KONFIGURÁCIA CESTY ---------
# Koreňový adresár s bronzovými dátami (tam, kde je priečinok "bronze")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(ROOT_DIR, "bronze")
OUTPUT_ROOT = os.path.join(ROOT_DIR, "silver_transform_merged0")
OUTPUT_FILE = os.path.join(OUTPUT_ROOT, "silver_merged.json")


# --------- POMOCNÉ FUNKCIE PRE NORMALIZÁCIU ---------

def normalize_radio_name(radio_dir_name: str) -> str:
    """Názov priečinka rádia na lowercase."""
    return radio_dir_name.lower()


def extract_time(value: str) -> Optional[str]:
    """
    Z reťazca typu 'YYYY-MM-DDTHH:MM:SS' alebo podobného vytiahne čas HH:MM:SS.
    Ak je value už len čas, len ho vráti.
    """
    if not isinstance(value, str):
        return None

    if "T" in value:
        value = value.split("T", 1)[1]
    elif " " in value:
        value = value.split(" ", 1)[1]

    parts = value.split(":")
    if len(parts) >= 2:
        hh = parts[0].zfill(2)
        mm = parts[1].zfill(2)
        ss = parts[2].zfill(2) if len(parts) >= 3 else "00"
        return f"{hh}:{mm}:{ss}"

    return None


def extract_date(value: str) -> Optional[str]:
    """
    Z reťazca typu 'YYYY-MM-DDTHH:MM:SS', 'YYYY-MM-DD', 'DD.MM.YYYY' atď.
    vytiahne dátum vo formáte DD.MM.YYYY.
    """
    if not isinstance(value, str):
        return None

    if "T" in value:
        value = value.split("T", 1)[0]
    elif " " in value:
        value = value.split(" ", 1)[0]

    if "-" in value:
        parts = value.split("-")
        if len(parts) >= 3:
            year = parts[0]
            month = parts[1]
            day = parts[2][:2]
            try:
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%d.%m.%Y")
            except ValueError:
                return None

    for fmt in ("%d.%m.%Y", "%Y.%m.%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%d.%m.%Y")
        except ValueError:
            continue

    return None


def normalize_title(record: Dict[str, Any]) -> Optional[str]:
    """Nájde názov skladby z viacerých možných kľúčov."""
    for key in ["title", "song", "musicTitle"]:
        if key in record and isinstance(record[key], str):
            return record[key]
    return None


def normalize_artists(record: Dict[str, Any]) -> List[str]:
    """
    Nájde autorov z kľúčov artists, musicAuthor, artist.
    Výstup je zoznam reťazcov.
    """
    for key in ["artists", "musicAuthor", "artist"]:
        if key in record:
            val = record[key]
            if isinstance(val, list):
                return [str(a) for a in val]
            elif isinstance(val, str):
                if "," in val:
                    return [a.strip() for a in val.split(",") if a.strip()]
                if "&" in val:
                    return [a.strip() for a in val.split("&") if a.strip()]
                return [val.strip()]
    return []


def normalize_time(record: Dict[str, Any]) -> Optional[str]:
    """
    Získa čas z kľúčov: start_time, startTime, play_time, time.
    Pri start_time sa berie len časová časť.
    """
    for key in ["start_time", "startTime", "play_time", "time"]:
        if key in record:
            return extract_time(str(record[key]))
    return None


def normalize_date(record: Dict[str, Any]) -> Optional[str]:
    """
    Získa dátum z kľúčov: start_time, recorded_at, play_date, date, last_update.
    Pri start_time / recorded_at / last_update sa berie len dátumová časť.
    """
    for key in ["start_time", "recorded_at", "play_date", "date", "last_update"]:
        if key in record:
            return extract_date(str(record[key]))
    return None


def get_song_session_id(record: Dict[str, Any]) -> Optional[str]:
    """Vráti song_session_id bez zmeny, ak existuje."""
    val = record.get("song_session_id")
    if val is None:
        return None
    return str(val)


def get_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Zjednotí tvar záznamu:
    - ak existuje vnorený objekt 'song', pracuje sa primárne s ním (ROCK, JAZZ, VLNA, niektoré FUNRADIO)
    - relevantné polia z vonkajšej úrovne sa doplnia, ak chýbajú vo vnútri
    """
    if isinstance(rec.get("song"), dict):
        inner = rec["song"].copy()
    else:
        inner = rec.copy()

    for k in ["start_time", "recorded_at", "play_date", "play_time",
              "time", "date", "last_update", "song_session_id"]:
        if k in rec and k not in inner:
            inner[k] = rec[k]

    return inner


# --------- HLAVNÁ EXTRAKCIA ---------

def process_json_file(file_path: str, radio_name: str) -> List[Dict[str, Any]]:
    """
    Načíta jeden JSON súbor a vráti zoznam normalizovaných záznamov.
    Súbor môže obsahovať buď zoznam, alebo 1 objekt.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return []

    records: List[Dict[str, Any]] = []
    iterable = data if isinstance(data, list) else [data]

    for rec in iterable:
        if not isinstance(rec, dict):
            continue

        payload = get_payload(rec)

        title = normalize_title(payload)
        artists = normalize_artists(payload)
        time_val = normalize_time(payload)
        date_val = normalize_date(payload)
        song_session_id = get_song_session_id(payload)

        # fallback: ak sa dátum nenašiel v payload, skús recorded_at na vonkajšej úrovni
        if not date_val and "recorded_at" in rec:
            date_val = extract_date(str(rec["recorded_at"]))

        if not title or not time_val or not date_val:
            continue

        normalized = {
            "radio": radio_name,
            "title": title,
            "artists": artists,
            "time": time_val,
            "date": date_val,
        }
        if song_session_id is not None:
            normalized["song_session_id"] = song_session_id

        records.append(normalized)

    return records


def walk_bronze_and_collect() -> List[Dict[str, Any]]:
    """
    Prejde štruktúru:
      bronze /
        RADIO /
          listeners / ... (ignorovať)
          song /
            DATE_DIR /
              *.json
    a vráti zoznam všetkých normalizovaných záznamov.
    """
    all_records: List[Dict[str, Any]] = []

    if not os.path.isdir(BRONZE_DIR):
        raise FileNotFoundError(f"Adresár {BRONZE_DIR} neexistuje")

    for radio_dir_name in os.listdir(BRONZE_DIR):
        radio_path = os.path.join(BRONZE_DIR, radio_dir_name)
        if not os.path.isdir(radio_path):
            continue

        radio_name = normalize_radio_name(radio_dir_name)

        song_root = os.path.join(radio_path, "song")
        if not os.path.isdir(song_root):
            continue

        for date_dir_name in os.listdir(song_root):
            date_dir_path = os.path.join(song_root, date_dir_name)
            if not os.path.isdir(date_dir_path):
                continue

            for fname in os.listdir(date_dir_path):
                if not fname.lower().endswith(".json"):
                    continue
                fpath = os.path.join(date_dir_path, fname)
                records = process_json_file(fpath, radio_name)
                all_records.extend(records)

    return all_records


# --------- ULOŽENIE VÝSLEDKU ---------

def ensure_output_dir():
    """Vytvorí výstupný koreňový adresár, ak neexistuje."""
    os.makedirs(OUTPUT_ROOT, exist_ok=True)


def save_merged_json(records: List[Dict[str, Any]]):
    """Uloží všetky záznamy do bronze_transform_merged/bronze_merged.json."""
    ensure_output_dir()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


# --------- MAIN ---------

def main():
    records = walk_bronze_and_collect()
    save_merged_json(records)


if __name__ == "__main__":
    main()
