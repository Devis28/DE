# -*- coding: utf-8 -*-
import re, math, hashlib, random, os
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Konštanty a HTTP
# ---------------------------------------------------------------------------
TZ  = ZoneInfo("Europe/Bratislava")
URL = "https://www.radia.sk/radia/melody/playlist"
UA  = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")

# Live bucket pre /now (koľko sekúnd držať „pomalý“ jitter)
SLOW_BUCKET_S = int(os.environ.get("SLOW_BUCKET_S", "30"))

# Denné špičky a minimá (reálne rozsahy)
WEEKDAY_PEAK  = float(os.environ.get("WEEKDAY_PEAK", 3200.0))
WEEKEND_PEAK  = float(os.environ.get("WEEKEND_PEAK", 2000.0))
NIGHT_MIN     = float(os.environ.get("NIGHT_MIN",  180.0))

# Jittery (percentá ako štandardná odchýlka a limity)
SLOW_SIGMA = float(os.environ.get("SLOW_SIGMA", "0.04"))  # ±4 % typicky
SLOW_CLIP  = float(os.environ.get("SLOW_CLIP",  "0.08"))  # max ±8 %
FAST_SIGMA = float(os.environ.get("FAST_SIGMA", "0.02"))  # ±2 %
FAST_CLIP  = float(os.environ.get("FAST_CLIP",  "0.04"))  # max ±4 %

# Bazál dennej krivky nikdy nie je presne 0 (aby sme neuviazli na minime)
BASE_FLOOR01 = float(os.environ.get("BASE_FLOOR01", "0.01"))  # 1 %
# Absolútne „vlnenie“ blízko podlahy, škáluje sa s (1 - fáza)
NIGHT_WIGGLE = float(os.environ.get("NIGHT_WIGGLE", "12.0"))  # skúšobne 12
# Nočná volatilita: násobí percentuálne jittre podľa (1 - fáza)
NIGHT_VOLATILITY = float(os.environ.get("NIGHT_VOLATILITY", "1.8"))  # 0=off, 1.8≈+180% pri podlahe
# Rýchlosť ditheru pre zaokrúhlenie (koľkokrát za sekundu nový seed)
DITHER_HZ = float(os.environ.get("DITHER_HZ", "2.0"))

def _fetch_with_requests():
    headers = {"User-Agent": UA, "Accept-Language": "sk,en;q=0.9"}
    r = requests.get(URL, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

def fetch_html():
    """Najprv requests; ak sa bráni, a máme cloudscraper, použijeme ho."""
    try:
        return _fetch_with_requests()
    except Exception:
        try:
            import cloudscraper
            s = cloudscraper.create_scraper()
            r = s.get(URL, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            raise e

# ---------------------------------------------------------------------------
# Parsovanie dátumu
# ---------------------------------------------------------------------------
def parse_date_label(lbl: str) -> date:
    t = lbl.strip().lower()
    today = datetime.now(TZ).date()
    if t.startswith("dnes"):
        return today
    if t.startswith("včera") or t.startswith("vcera"):
        return today - timedelta(days=1)
    m = re.search(r"(\d{2}\.\d{2}\.\d{4})", t)
    if m:
        return datetime.strptime(m.group(1), "%d.%m.%Y").date()
    return today

def fmt_date(d: date) -> str:
    return d.strftime("%d.%m.%Y")

# ---------------------------------------------------------------------------
# Denná krivka (zjednodušené, ale realistické)
# ---------------------------------------------------------------------------
def _gauss(x, mu, sigma, amp):
    return amp * math.exp(-0.5 * ((x - mu) / sigma) ** 2)

def _shape_weekday_raw(h):
    # ráno (7:45–9), obed, popoludní peak, večer menší nábeh
    return (
        _gauss(h, 7.9, 1.2, 0.9) +
        _gauss(h, 12.5, 1.3, 0.45) +
        _gauss(h, 17.3, 1.3, 0.85) +
        _gauss(h, 20.3, 1.8, 0.35)
    )

def _shape_weekend_raw(h):
    # víkend: neskorší nábeh, silné popoludnie
    return (
        _gauss(h, 10.0, 1.7, 0.35) +
        _gauss(h, 14.0, 2.0, 0.95) +
        _gauss(h, 19.5, 2.0, 0.55)
    )

def _normalize(arr):
    lo, hi = min(arr), max(arr)
    if hi <= lo: return [0.0 for _ in arr]
    return [(v - lo) / (hi - lo) for v in arr]

def _day_norm(is_weekend: bool):
    grid = [i/12 for i in range(0, 24*12 + 1)]  # 5-min mriežka
    raw = [(_shape_weekend_raw(x) if is_weekend else _shape_weekday_raw(x)) for x in grid]
    return grid, _normalize(raw)

def _night_depressor(h):
    # silný útlm okolo 02:30 (0.2..1.0)
    valley = math.exp(-0.5 * ((h - 2.5) / 2.0) ** 2)
    return max(0.2, 1.0 - 0.8 * valley)

def _expected_count(dt: datetime) -> float:
    """Očakávaný počet podľa dennej krivky + nočný útlm, škálovaný na [NIGHT_MIN, PEAK]."""
    h = dt.hour + dt.minute/60.0
    is_weekend = dt.weekday() >= 5
    key = "we" if is_weekend else "wd"
    if not hasattr(_expected_count, "_cache"):
        _expected_count._cache = {}
    if key not in _expected_count._cache:
        _expected_count._cache[key] = _day_norm(is_weekend)
    grid, norm = _expected_count._cache[key]
    idx = min(range(len(grid)), key=lambda i: abs(grid[i] - h))
    base01 = (BASE_FLOOR01 + (1.0 - BASE_FLOOR01) * norm[idx]) * _night_depressor(h)
    peak = WEEKEND_PEAK if is_weekend else WEEKDAY_PEAK
    return NIGHT_MIN + base01 * (peak - NIGHT_MIN)

# ---------------------------------------------------------------------------
# Jittery + dither zaokrúhlenie
# ---------------------------------------------------------------------------
def _gauss_jitter(seed_int: int, sigma: float, clip: float) -> float:
    """vygeneruje N(0,sigma) z deterministického seed-u a oreže na ±clip"""
    rng = random.Random(seed_int)
    u1, u2 = max(rng.random(), 1e-9), max(rng.random(), 1e-9)
    z = ((-2.0 * math.log(u1)) ** 0.5) * math.cos(2*math.pi*u2)
    eps = max(-clip, min(clip, sigma * z))
    return eps

def _slow_jitter(song_key: str, now_ts: float) -> float:
    """mení sa po SLOW_BUCKET_S; deterministický podľa skladby"""
    bucket = int(now_ts // max(1, SLOW_BUCKET_S))
    seed = int(hashlib.md5(f"{song_key}|{bucket}".encode("utf-8")).hexdigest()[:16], 16)
    return _gauss_jitter(seed, SLOW_SIGMA, SLOW_CLIP)

def _fast_jitter(ts_ms: int | None) -> float:
    """jemný jitter na každý tick; ak ts chýba, použijeme aktuálne ms"""
    if ts_ms is None:
        ts_ms = int(datetime.now(TZ).timestamp() * 1000)
    seed = int(hashlib.sha1(str(ts_ms).encode("utf-8")).hexdigest()[:16], 16)
    return _gauss_jitter(seed, FAST_SIGMA, FAST_CLIP)

def _stochastic_round(x: float, seed_key: str, now_ts: float) -> int:
    """Ditherované zaokrúhlenie: pravdepodobnosťou podľa frakcie zaokrúhli nahor."""
    floor = math.floor(x)
    frac = x - floor
    # seed meníme DITHER_HZ-krát za sekundu (deterministicky)
    tick = int(now_ts * max(1.0, DITHER_HZ))
    seed = int(hashlib.sha1(f"{seed_key}|{tick}".encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)
    return floor + (1 if rng.random() < frac else 0)

# ---------------------------------------------------------------------------
# Verejné API: estimate_listeners (BACKWARD-COMPATIBLE)
# ---------------------------------------------------------------------------
def estimate_listeners(dt: datetime,
                       seed_key: str | None = None,
                       ts_ms: int | None = None,
                       debug: bool = False) -> int | dict:
    """
    Výsledok = denná krivka (podľa dt) + jitter:
      • jitter sa uplatňuje viac v noci (NIGHT_VOLATILITY)
      • pridá sa absolútna prísada (NIGHT_WIGGLE) ~ len blízko podlahy
      • použije sa ditherované zaokrúhlenie, aby nevznikali „ploché“ čísla
    """
    base = _expected_count(dt)

    # fallback pre staré volania bez seed_key (stabilné v rámci minúty)
    if seed_key is None:
        seed_key = f"fallback|{dt.strftime('%Y-%m-%d %H:%M')}"

    now_ts = datetime.now(TZ).timestamp()
    slow = _slow_jitter(seed_key, now_ts)
    fast = _fast_jitter(ts_ms)

    peak_cap = WEEKEND_PEAK if dt.weekday() >= 5 else WEEKDAY_PEAK

    # fáza 0..1: 0 = pri podlahe, 1 = pri peaku
    phase = 0.0
    if peak_cap > NIGHT_MIN:
        phase = max(0.0, min(1.0, (base - NIGHT_MIN) / (peak_cap - NIGHT_MIN)))

    # v noci zvýšime percentuálne jittre
    vol_mult = 1.0 + NIGHT_VOLATILITY * (1.0 - phase)
    slow *= vol_mult
    fast *= vol_mult

    # jitter na „nadpodlažnú“ časť
    above = max(0.0, base - NIGHT_MIN)
    v = NIGHT_MIN + above * (1.0 + slow + fast)

    # absolútne vlnenie, silné pri podlahe, mizne v špičke
    if NIGHT_WIGGLE > 0.0:
        v += (NIGHT_WIGGLE * (1.0 - phase)) * (slow + fast)

    # orez a ditherované zaokrúhlenie
    v = max(NIGHT_MIN, min(peak_cap, v))
    out = _stochastic_round(v, seed_key, now_ts)

    if not debug:
        return out
    return {
        "value": out,
        "_dbg": {
            "base": round(base, 2),
            "slow": round(slow, 4),
            "fast": round(fast, 4),
            "phase": round(phase, 3),
            "vol_mult": round(vol_mult, 3),
            "v_clamped": round(v, 2),
            "peak_cap": peak_cap,
            "night_min": NIGHT_MIN
        }
    }

# ---------------------------------------------------------------------------
# Parsovanie prvej (aktuálnej) položky
# ---------------------------------------------------------------------------
def parse_first_row(html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    row = soup.select_one("div.row.data, div.row_data")
    if not row: return None

    d_el = row.select_one(".datum")
    t_el = row.select_one(".cas")
    a_el = row.select_one(".interpret")
    s_el = row.select_one(".titul")
    if not all([d_el, t_el, a_el, s_el]): return None

    d = parse_date_label(d_el.get_text())
    hh, mm = [int(x) for x in t_el.get_text().strip().split(":")]
    tm = time(hour=hh, minute=mm)

    return {
        "title":  s_el.get_text(strip=True),
        "artist": a_el.get_text(strip=True),
        "date":   fmt_date(d),
        "time":   tm.strftime("%H:%M"),
        # listeners dorátame až pri get_now_playing()
    }

# ---------------------------------------------------------------------------
# Public API: now-playing
# ---------------------------------------------------------------------------
def get_now_playing(override_ts: int | None = None, debug: bool = False) -> dict:
    html = fetch_html()
    row = parse_first_row(html)
    if not row:
        return {"error": "Nepodarilo sa získať aktuálnu skladbu."}

    d = datetime.strptime(row["date"], "%d.%m.%Y").date()
    hh, mm = [int(x) for x in row["time"].split(":")]
    dtp = datetime.combine(d, time(hour=hh, minute=mm), TZ)

    song_key = f'{row["artist"]}|{row["title"]}|{row["date"]}|{row["time"]}'
    res = estimate_listeners(dtp, seed_key=song_key, ts_ms=override_ts, debug=debug)

    if debug:
        row["listeners"] = res["value"]
        row["_dbg"] = res["_dbg"]
    else:
        row["listeners"] = res
    return row
