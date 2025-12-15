import os
import json
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(r"C:\Users\david\PycharmProjects\radioETL")
BRONZE_DIR = BASE_DIR / "bronze"
OUTPUT_DIR = BASE_DIR / "silver_transform_merged1"
OUTPUT_FILE = OUTPUT_DIR / "merged_listeners.json"

IGNORED_SUBDIRS = {"song"}

TARGET_FORMAT = "%d.%m.%Y %H:%M:%S"  # 31.10.2025 22:57:08

def normalize_recorded_at(value: str | None) -> str | None:
    if not value:
        return None

    value = value.strip()

    # Už v cieľovom formáte
    try:
        dt = datetime.strptime(value, TARGET_FORMAT)
        return dt.strftime(TARGET_FORMAT)
    except ValueError:
        pass

    # ISO formát s offsetom: 2025-11-14T21:48:43.641590+01:00
    try:
        # od Pythonu 3.11 vie datetime.fromisoformat priamo toto
        dt = datetime.fromisoformat(value)
        return dt.strftime(TARGET_FORMAT)
    except ValueError:
        pass

    # prípadný fallback – nechaj pôvodnú hodnotu, ak sa nedá parse-núť
    return value


def collect_listeners():
    merged = []

    for radio_dir in BRONZE_DIR.iterdir():
        if not radio_dir.is_dir():
            continue

        for subdir in radio_dir.iterdir():
            if not subdir.is_dir():
                continue
            if subdir.name.lower() in IGNORED_SUBDIRS:
                continue

            for day_dir in subdir.rglob("*"):
                if not day_dir.is_dir():
                    continue

                for json_path in day_dir.glob("*.json"):
                    with open(json_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    records = data if isinstance(data, list) else [data]

                    for rec in records:
                        recorded_at = rec.get("recorded_at")
                        merged.append({
                            "listeners": rec.get("listeners"),
                            "song_session_id": rec.get("song_session_id"),
                            "recorded_at": normalize_recorded_at(recorded_at),
                        })

    return merged


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    merged = collect_listeners()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"Uložených záznamov: {len(merged)}")
    print(f"Výstup: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
