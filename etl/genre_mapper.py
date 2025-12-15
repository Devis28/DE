import json
import re
from pathlib import Path

ALLOWED = {
    "pop", "rock", "hip hop", "rap", "r&b", "soul", "metal", "jazz", "blues",
    "electronic", "house", "techno", "trance", "folk", "country", "punk",
    "reggae", "funk", "indie", "alternative", "dance", "classical", "disco",
    "latin", "world"
}

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

DIRECT_MAP = {
    "hip-hop/rap": "hip hop",
    "hip hop/rap": "hip hop",
    "hip-hop": "hip hop",
    "hip hop": "hip hop",

    "rnb": "r&b",
    "r and b": "r&b",
    "rhythm and blues": "r&b",

    "alt": "alternative",
    "alt rock": "alternative",
    "alternative rock": "alternative",

    "edm": "electronic",
    "electronica": "electronic",
    "electro": "electronic",

    "dance pop": "dance",
    "dance-pop": "dance",

    "indie rock": "indie",
    "indie pop": "indie",

    "hard rock": "rock",
    "soft rock": "rock",
    "classic rock": "rock",

    "heavy metal": "metal",
    "black metal": "metal",
    "death metal": "metal",
    "thrash metal": "metal",
    "metalcore": "metal",

    "punk rock": "punk",
    "pop punk": "punk",

    "reggaeton": "latin",
    "latin pop": "latin",
    "latin rock": "latin",

    "afrobeat": "world",
    "afrobeats": "world",
    "world music": "world",

    "ambient": "electronic",
    "downtempo": "electronic",
    "drum and bass": "electronic",
    "dnb": "electronic",
    "dubstep": "electronic",

    "trap": "rap",

    "deep house": "house",
    "tech house": "house",
    "progressive house": "house",

    "minimal techno": "techno",

    "progressive trance": "trance",
    "psytrance": "trance",

    "bluegrass": "country",
    "americana": "country",
    "singer-songwriter": "folk",

    "neo soul": "soul",

    "baroque": "classical",
    "romantic": "classical",
    "opera": "classical",

    "ost": "classical",
    "score": "classical",
    "soundtrack": "classical",

    "bossa nova": "latin",
}

def map_to_allowed(raw: str) -> str | None:
    g = norm(raw)
    if not g:
        return None

    if g in ALLOWED:
        return g

    if g in DIRECT_MAP and DIRECT_MAP[g] in ALLOWED:
        return DIRECT_MAP[g]

    parts = re.split(r"[\/,&;|]+", g)
    for p in map(norm, parts):
        if p in ALLOWED:
            return p
        if p in DIRECT_MAP and DIRECT_MAP[p] in ALLOWED:
            return DIRECT_MAP[p]

    if "hip" in g and "hop" in g:
        return "hip hop"
    if "rap" in g:
        return "rap"
    if "metal" in g:
        return "metal"
    if "punk" in g:
        return "punk"
    if "reggae" in g:
        return "reggae"
    if "funk" in g:
        return "funk"
    if "blues" in g:
        return "blues"
    if "jazz" in g:
        return "jazz"
    if "house" in g:
        return "house"
    if "techno" in g:
        return "techno"
    if "trance" in g:
        return "trance"
    if "disco" in g:
        return "disco"
    if "dance" in g:
        return "dance"
    if "electro" in g or "edm" in g:
        return "electronic"
    if "indie" in g:
        return "indie"
    if "alternat" in g:
        return "alternative"
    if "country" in g:
        return "country"
    if "folk" in g:
        return "folk"
    if "latin" in g or "reggaeton" in g:
        return "latin"
    if "rock" in g:
        return "rock"
    if "pop" in g:
        return "pop"

    return None  # nič nesedí → necháme pôvodné

def main():
    in_path = Path(r"silver_transform_merged1\silver_enrich_durationsec.json")
    out_path = Path(r"silver_transform_merged1\silver_enrich_durationsec_genresOK2.json")

    with in_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    changed = 0
    kept_original = 0

    for item in data:
        raw = item.get("genre")
        if not raw:
            continue

        mapped = map_to_allowed(raw)
        if mapped is None:
            kept_original += 1
            continue

        if norm(raw) != mapped:
            changed += 1
        item["genre"] = mapped

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"OK: {out_path}")
    print(f"Záznamov: {len(data)}")
    print(f"Premapovaných: {changed}")
    print(f"Ponechaných pôvodných (bez matchu): {kept_original}")

if __name__ == "__main__":
    main()
