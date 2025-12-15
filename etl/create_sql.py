import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable

# ====== CESTY ======
DDL_SQL_PATH = r"schema_radioDB.sql"  # Workbench DDL
MAIN_JSON_PATH = r"silver_transform_merged1/silver_enrich_durationsec_genresOK.json"
LISTENERS_JSON_PATH = r"silver_transform_merged1/merged_listeners.json"
OUT_SQL_PATH = r"radioDB_full_load.sql"

DB_SCHEMA = "radioDB"

# Rádio -> headquarters
HQ_MAP = {
    "vlna": "Bratislava",
    "melody": "Bratislava",
    "expres": "Bratislava",
    "jazz": "Bratislava",
    "rock": "Bratislava",
    "funradio": "Bratislava",
}
DEFAULT_HEADQUARTERS = "UNKNOWN"

# ====== POMOCNÉ FUNKCIE ======
def load_json(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))

def iter_records(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, list):
        for x in obj:
            if isinstance(x, dict):
                yield x
    elif isinstance(obj, dict):
        yield obj

def sql_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")

def norm_genre(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    return s[:1].upper() + s[1:].lower()

def artists_to_str(a: Any) -> str:
    if a is None:
        return ""
    if isinstance(a, list):
        return ", ".join(str(x).strip() for x in a if str(x).strip())
    return str(a).strip()

def as_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None

def parse_played_at(date_str: Any, time_str: Any) -> Optional[datetime]:
    if not date_str or not time_str:
        return None
    try:
        return datetime.strptime(f"{str(date_str).strip()} {str(time_str).strip()}", "%d.%m.%Y %H:%M:%S")
    except ValueError:
        return None

def parse_recorded_at(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    try:
        return datetime.strptime(str(s).strip(), "%d.%m.%Y %H:%M:%S")
    except ValueError:
        return None

def dt_to_mysql(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ====== DDL SANITIZER ======
def sanitize_workbench_ddl(ddl: str) -> str:
    ddl = re.sub(r"\s+VISIBLE\b", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\s+ASC\b", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\s+DESC\b", "", ddl, flags=re.IGNORECASE)
    return ddl

def read_and_sanitize_ddl(path: str) -> str:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"DDL file not found: {path}")
    return sanitize_workbench_ddl(p.read_text(encoding="utf-8"))

# ====== GENEROVANIE SQL ======
def main():
    ddl_text = read_and_sanitize_ddl(DDL_SQL_PATH)

    main_rows = list(iter_records(load_json(MAIN_JSON_PATH)))
    listener_rows = list(iter_records(load_json(LISTENERS_JSON_PATH)))

    sql: List[str] = []

    # 0) DDL
    sql.extend([
        "-- =====================",
        "-- DDL (sanitized from Workbench)",
        "-- =====================",
        ddl_text.strip(),
        "",
        "-- =====================",
        "-- DML (generated inserts)",
        "-- =====================",
        "",
        f"USE `{DB_SCHEMA}`;",
        "SET FOREIGN_KEY_CHECKS=0;",
        ""
    ])

    seen_genres = set()
    seen_radios = set()
    seen_songs = set()
    seen_sessions = set()

    def emit_genre(genre: str):
        g = norm_genre(genre)
        if not g:
            return
        key = g.lower()
        if key in seen_genres:
            return
        seen_genres.add(key)

        ge = sql_escape(g)
        sql.extend([
            f"-- genre: {g}",
            "INSERT INTO genre(genre)",
            f"SELECT '{ge}' WHERE NOT EXISTS (SELECT 1 FROM genre WHERE genre='{ge}');",
            f"SET @genre_id := (SELECT id FROM genre WHERE genre='{ge}' LIMIT 1);",
            ""
        ])

    def emit_radio(radio_name: str, radio_genre: str):
        name = (radio_name or "").strip()
        if not name:
            return

        name_lc = name.lower()
        hq = HQ_MAP.get(name_lc, DEFAULT_HEADQUARTERS)
        g = norm_genre(radio_genre)

        key = (name_lc, hq.lower(), g.lower())
        if key in seen_radios:
            return
        seen_radios.add(key)

        emit_genre(g)

        ne = sql_escape(name)
        he = sql_escape(hq)
        ge = sql_escape(g)

        sql.extend([
            f"-- radio: {name}",
            f"SET @genre_id := (SELECT id FROM genre WHERE genre='{ge}' LIMIT 1);",
            "INSERT INTO radio(name, headquarters, genre_id)",
            f"SELECT '{ne}', '{he}', @genre_id",
            "WHERE NOT EXISTS (",
            f"  SELECT 1 FROM radio WHERE name='{ne}' AND headquarters='{he}' AND genre_id=@genre_id",
            ");",
            "SET @radio_id := (",
            f"  SELECT id FROM radio WHERE name='{ne}' AND headquarters='{he}' AND genre_id=@genre_id",
            "  ORDER BY id ASC LIMIT 1",
            ");",
            ""
        ])

    def emit_song(title: str, artists: str, duration: Optional[int], release_year: Optional[int], song_genre: str):
        t = (title or "").strip()
        a = (artists or "").strip()
        g = norm_genre(song_genre)
        if not t or not a or not g:
            return

        emit_genre(g)

        key = (t.lower(), a.lower(), str(release_year or ""), g.lower())
        if key in seen_songs:
            # Aj keď sme song už riešili predtým, musíme nastaviť @song_id na existujúci row
            te = sql_escape(t)
            ae = sql_escape(a)
            ge = sql_escape(g)
            yr_sql = "NULL" if release_year is None else str(int(release_year))
            sql.extend([
                f"SET @genre_id := (SELECT id FROM genre WHERE genre='{ge}' LIMIT 1);",
                "SET @song_id := (",
                "  SELECT id FROM song",
                f"  WHERE title='{te}' AND artists='{ae}'",
                f"    AND ( (release_year IS NULL AND {yr_sql} IS NULL) OR release_year={yr_sql} )",
                "    AND genre_id=@genre_id",
                "  ORDER BY id ASC LIMIT 1",
                ");",
                ""
            ])
            return

        seen_songs.add(key)

        te = sql_escape(t)
        ae = sql_escape(a)
        ge = sql_escape(g)
        dur_sql = "NULL" if duration is None else str(int(duration))
        yr_sql = "NULL" if release_year is None else str(int(release_year))

        sql.extend([
            f"-- song: {t} / {a}",
            f"SET @genre_id := (SELECT id FROM genre WHERE genre='{ge}' LIMIT 1);",
            "INSERT INTO song(title, artists, duration, release_year, genre_id)",
            f"SELECT '{te}', '{ae}', {dur_sql}, {yr_sql}, @genre_id",
            "WHERE NOT EXISTS (",
            f"  SELECT 1 FROM song",
            f"  WHERE title='{te}' AND artists='{ae}'",
            f"    AND ( (release_year IS NULL AND {yr_sql} IS NULL) OR release_year={yr_sql} )",
            "    AND genre_id=@genre_id",
            ");",
            "SET @song_id := (",
            "  SELECT id FROM song",
            f"  WHERE title='{te}' AND artists='{ae}'",
            f"    AND ( (release_year IS NULL AND {yr_sql} IS NULL) OR release_year={yr_sql} )",
            "    AND genre_id=@genre_id",
            "  ORDER BY id ASC LIMIT 1",
            ");",
            ""
        ])

    def emit_session(song_session_uuid: str, played_at: datetime):
        su = (song_session_uuid or "").strip()
        if not su:
            return

        if su in seen_sessions:
            return
        seen_sessions.add(su)

        sue = sql_escape(su)
        pae = sql_escape(dt_to_mysql(played_at))

        sql.extend([
            f"-- song_session: {su}",
            "INSERT INTO song_session(song_session_uuid, song_id, radio_id, played_at)",
            f"SELECT '{sue}', @song_id, @radio_id, '{pae}'",
            f"WHERE NOT EXISTS (SELECT 1 FROM song_session WHERE song_session_uuid='{sue}');",
            "SET @song_session_id := (",
            f"  SELECT id FROM song_session WHERE song_session_uuid='{sue}' LIMIT 1",
            ");",
            ""
        ])

    # 1) Insert dimenzie + sessions
    for r in main_rows:
        played_at = parse_played_at(r.get("date"), r.get("time"))
        if played_at is None:
            continue

        radio_name = str(r.get("radio", "")).strip()
        title = str(r.get("title", "")).strip()
        artists = artists_to_str(r.get("artists"))
        session_uuid = str(r.get("song_session_id", "")).strip()
        duration = as_int(r.get("duration"))
        release_year = as_int(r.get("release_year"))
        genre = norm_genre(r.get("genre"))

        emit_radio(radio_name, genre)
        emit_song(title, artists, duration, release_year, genre)
        emit_session(session_uuid, played_at)

    # 2) Insert listener measurements (s kontrolou existencie session a bez duplicit)
    sql.append("-- listener measurements")
    for r in listener_rows:
        sess_uuid = str(r.get("song_session_id", "")).strip()
        dt = parse_recorded_at(r.get("recorded_at"))
        listeners = as_int(r.get("listeners"))
        if not sess_uuid or dt is None or listeners is None:
            continue

        sess_e = sql_escape(sess_uuid)
        dt_e = sql_escape(dt_to_mysql(dt))

        sql.extend([
            f"SET @song_session_id := (SELECT id FROM song_session WHERE song_session_uuid='{sess_e}' LIMIT 1);",
            "SET @__ss_exists := IF(@song_session_id IS NULL, 0, 1);",
            "INSERT INTO listener_measurement(recorded_at, listeners, song_session_id)",
            f"SELECT '{dt_e}', {int(listeners)}, @song_session_id",
            "WHERE @__ss_exists = 1",
            "  AND NOT EXISTS (",
            "    SELECT 1 FROM listener_measurement",
            f"    WHERE song_session_id=@song_session_id AND recorded_at='{dt_e}'",
            "  );",
            ""
        ])

    sql.extend([
        "SET FOREIGN_KEY_CHECKS=1;",
        ""
    ])

    Path(OUT_SQL_PATH).write_text("\n".join(sql), encoding="utf-8")
    print(f"✅ OK: created {OUT_SQL_PATH}")

if __name__ == "__main__":
    main()
