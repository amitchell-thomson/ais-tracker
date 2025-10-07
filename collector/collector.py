#!/usr/bin/env python3
"""
Unified AIS Collector (tiles → normalized tanker fixes → Timescale/PostGIS)

    - Inserts only the columns your new triggers expect to enrich:
        ts, src, vessel_uid, lat, lon, sog, cog, heading, elapsed,
        destination, flag, length_m, width_m, dwt, shipname, shiptype, ship_id, rot
        (geom + memberships are set by BEFORE triggers in the DB.)
    - Filters to **tankers only** (based on SHIPTYPE family 8 or TYPE hints).
    - Computes a robust **vessel_uid**:
        MMSI/IMO/MTID > else hashed surrogate from (name, flag, len, width, type).
    - Optional **spatial prefilter** against buffered AREA/GATE bounding boxes.


"""

from __future__ import annotations
import os
import sys
import time
import json
import math
import uuid
import shutil
import hashlib
import random
import logging
import tempfile
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable
from itertools import cycle, islice
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler

# --------------------------- Runtime / 3-line UI ----------------------------
try:
    import colorama
    colorama.just_fix_windows_console()  # enable ANSI on Windows
except Exception:
    pass

USE_RICH = True
try:
    from rich.live import Live
    from rich.console import Console
    from rich.text import Text
    console = Console()
except Exception:
    USE_RICH = False
    console = None

UI_MODE = os.getenv("UI_MODE", "three_ansi").lower()  # three_ansi|three|single|off|auto
IS_TTY = sys.stdout.isatty()

def _use_three_ansi() -> bool:
    return UI_MODE == "three_ansi" and IS_TTY

# Disable Rich when we handle our own in-place updates
if _use_three_ansi():
    USE_RICH = False
# --- Single-line UI mode (for runners/loggers) ---
UI_MODE = os.getenv("UI_MODE", "auto").lower()  # auto|three|single|off
IS_TTY = sys.stdout.isatty()

def _use_single_line() -> bool:
    if UI_MODE == "single":
        return True
    if UI_MODE == "three":
        return False
    if UI_MODE == "off":
        return True  # suppress multi-line redraws; we'll only emit one updating line
    # auto: single-line when not a TTY (e.g., runners, logs)
    return not IS_TTY

# If we’re in single-line mode, don’t use Rich’s 3-line Live UI
if _use_single_line():
    USE_RICH = False


_setup_line  = ""
_latest_line = "(waiting for first cycle)"
_bottom_line = "(countdown)"
_live_obj    = None
_UI_ENABLED  = True
_printed_block = False  # have we printed the initial 3-line block?


def _view_text() -> str:
    return f"{_setup_line}\n{_latest_line}\n{_bottom_line}"

def _renderable():
    if USE_RICH:
        from rich.text import Text
        return Text(_view_text())
    return _view_text()

def _refresh():
    if not _UI_ENABLED:
        return

    # Fixed 3-line, in-place updater
    if _use_three_ansi():
        global _printed_block
        if not _printed_block:
            sys.stdout.write(_view_text() + "\n")
            sys.stdout.flush()
            _printed_block = True
            return

        # Move cursor to the start of the 3-line block and rewrite lines
        # \x1b[3F = move cursor up 3 lines, to column 1
        sys.stdout.write("\x1b[3F")  # up to SETUP line
        for line in (_setup_line, _latest_line, _bottom_line):
            sys.stdout.write("\x1b[2K")     # clear entire line
            sys.stdout.write(line + "\n")   # write updated content
        sys.stdout.flush()
        return

    # Fallbacks: single-line or 3-line full redraw
    if USE_RICH and _live_obj is not None:
        _live_obj.update(_renderable())
    else:
        os.system("cls" if os.name == "nt" else "clear")
        sys.stdout.write(_view_text() + "\n")
        sys.stdout.flush()

def ui_disable():
    global _UI_ENABLED
    if _UI_ENABLED and _use_three_ansi():
        # move cursor to next clean line when exiting
        sys.stdout.write("\n")
        sys.stdout.flush()
    _UI_ENABLED = False


def ui_set_setup(text: str):
    global _setup_line
    _setup_line = text
    _refresh()

def ui_set_latest(text: str):
    global _latest_line
    _latest_line = text
    _refresh()

def ui_set_bottom(text: str):
    global _bottom_line
    _bottom_line = text
    _refresh()

# ------------------------------ Configuration -------------------------------

PG_DSN              = "postgresql://ais:aispass@localhost:5432/ais"

CYCLES_PER_DRIVER   = 30
INTERVAL_SECONDS    = 150
TILES_PER_CYCLE     = 8
TILE_MODE           = "static"

# static tiles seed (z/x/y;z/x/y;...)
STATIC_TILES = (
    "5/5/5;5/5/6;5/6/5;5/6/6;5/8/9;5/9/9;6/7/13;6/8/13;6/11/11;6/11/12;"
    "6/11/13;6/12/11;6/12/12;6/12/13;6/15/12;6/16/10;6/17/19;6/18/13;6/18/19;6/20/13;"
    "6/21/13;7/14/26;7/15/26;7/16/26;7/32/20;7/32/21;7/33/20;7/40/26;7/40/27;7/41/27;7/42/27"
)

# Fetch / backoff
JITTER_SECONDS      = 10
MAX_RETRIES         = 0
BACKOFF_BASE_SECS   = 5.0
TILE_PAUSE_MS       = 2500
TILE_JITTER_MS      = 600

COOLDOWN_FAIL_RATIO = 0.4
COOLDOWN_SECONDS    = 120
COOLDOWN_JITTER     = 60
REOPEN_FAIL_RATIO   = 0.6

# Normalization cuts / tanker predicate / SAT continuity
DROP_IF_OBS_AGE_MIN = 720                                                  # drop >12h old
MAX_TANKER_SOG_KN   = 35.0                                                 # sanity cap

# Prefilter by AREA/GATE bounding boxes (in degrees)
ENABLE_BBOX_PREFILTER   = True
PREFETCH_BUFFER_DEG     = 0.15                                             # ~15-20km at mid lat

# ------------------------------- Logging ------------------------------------
def setup_logging() -> logging.Logger:
    Path("logs").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("collector")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    fh = TimedRotatingFileHandler(
        "logs/collector.log", when="midnight", backupCount=14, utc=True, encoding="utf-8"
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
    logger.propagate = False
    return logger

logger = setup_logging()

# ------------------------------ Selenium driver -----------------------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

def open_driver(minimized: bool = False, size=(400, 300)) -> webdriver.Chrome:
    """
    Launches a clean Chrome session with a unique temp profile so we can
    reliably kill just our processes on Windows if needed.
    """
    opts = Options()
    opts.add_argument("--disable-background-mode")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    # Silence automation banners/logging a bit
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)

    profile_dir = tempfile.mkdtemp(prefix="selenium-profile-")
    opts.add_argument(f"--user-data-dir={profile_dir}")

    marker = f"selenium-session-{uuid.uuid4().hex}"
    opts.add_argument(f"--{marker}")

    service = Service(log_output=subprocess.DEVNULL)
    driver = webdriver.Chrome(options=opts, service=service)

    # Attach metadata for cleanup later
    driver._profile_dir = profile_dir
    driver._marker = marker
    driver._service = service

    try:
        driver.set_window_size(*size)
        if minimized:
            driver.set_window_position(-2000, 0)
    except Exception:
        pass
    return driver

def _kill_chrome_by_signature(profile_dir: str, marker: str):
    """
    Windows helper: kill chrome.exe processes whose command line contains our
    unique marker or our temp profile. No-ops on non-Windows.
    """

    prof_re = profile_dir.replace("\\", "\\\\")
    mark_re = marker
    ps_script = (
        "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
        f"Where-Object {{ $_.CommandLine -match '{mark_re}' -or $_.CommandLine -match '{prof_re}' }} | "
        "Select-Object -ExpandProperty ProcessId"
    )
    try:
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", ps_script],
            text=True, stderr=subprocess.DEVNULL
        )
        for pid in [p.strip() for p in out.splitlines() if p.strip().isdigit()]:
            subprocess.run(["taskkill", "/F", "/PID", pid], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

def safe_quit(driver: Optional[webdriver.Chrome]):
    """
    Best-effort Chrome shutdown + hard kill (Windows) + temp profile cleanup.
    """
    if not driver:
        return
    profile_dir = getattr(driver, "_profile_dir", None)
    marker      = getattr(driver, "_marker", None)
    try:
        driver.quit()
    except Exception:
        pass
    time.sleep(0.3)
    if profile_dir or marker:
        _kill_chrome_by_signature(profile_dir or "", marker or "")
    if profile_dir:
        try:
            import shutil as _sh
            _sh.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass

def _extract_json_from_dom(driver: webdriver.Chrome) -> Optional[dict]:
    """
    The MarineTraffic tile endpoint renders JSON directly; grab body/pre text and parse.
    """
    pres = driver.find_elements(By.TAG_NAME, "pre")
    if pres:
        text = pres[0].text
    else:
        text = driver.execute_script("return document.body ? document.body.innerText : '';") or ""
    text = text.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None

def fetch_once(driver: webdriver.Chrome, url: str, wait_seconds: int = 10) -> Optional[dict]:
    driver.get(url)
    WebDriverWait(driver, wait_seconds).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    return _extract_json_from_dom(driver)

def tile_url(z: int, x: int, y: int) -> str:
    # Note: this is your previous endpoint; update path params if you add SAT/alt layers
    return f"https://www.marinetraffic.com/getData/get_data_json_4/z:{z}/X:{x}/Y:{y}/station:0"

# ------------------------------- Tiles --------------------------------------
def parse_tile_str(s: str) -> Tuple[int,int,int]:
    z, x, y = s.strip().split("/")
    return int(z), int(x), int(y)

def build_tile_cycle() -> Tuple[Iterable[Tuple[int,int,int]], int]:
    """
    Currently supports only static tile lists (env: STATIC_TILES).
    See suggestions to build tiles dynamically from AREA/GATE bboxes.
    """
    if TILE_MODE != "static":
        raise ValueError("Only TILE_MODE=static is implemented in this collector.")
    tiles = [t for t in (p.strip() for p in STATIC_TILES.split(";")) if t]
    if not tiles:
        raise ValueError("STATIC_TILES is empty.")
    return cycle([parse_tile_str(t) for t in tiles]), len(tiles)

# ------------------------------ Normalization -------------------------------
def _to_float(x: Any) -> Optional[float]:
    if x is None: return None
    try: return float(str(x).strip())
    except Exception: return None

def _to_int(x: Any) -> Optional[int]:
    if x is None: return None
    try: return int(float(str(x).strip()))
    except Exception: return None

def _to_str(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s or None

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _norm(v: Any) -> str:
    """Uppercase string normalization for hashing."""
    if v is None: return ""
    if isinstance(v, float) and math.isnan(v): return ""
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}: return ""
    return s.upper()

def classify_src(row: Dict[str, Any]) -> str:
    """
    Classify data source as 'sat' or 'terrestrial' for stamping in ais_fix.src.
    """
    name = (row.get("SHIPNAME") or "").strip().upper()
    ship_id = str(row.get("SHIP_ID") or "")
    # crude SAT hints: placeholder name or opaque/base64-ish ship_id tokens
    looks_opaque = (any(c in ship_id for c in ("=", "/", "+")) and len(ship_id) > 16) or not ship_id.isdigit()
    if name == "[SAT-AIS]" or looks_opaque:
        return "sat"
    return "terrestrial"

def tanker_predicate(row: Dict[str, Any]) -> bool:
    """
    Return True if the row looks like a tanker.
    Priority: AIS SHIPTYPE family 8; else TYPE_NAME/TYPE_IMG; else GT_SHIPTYPE=17 (as seen).
    """
    stype = _to_str(row.get("SHIPTYPE")) or ""
    if stype and stype.strip().startswith("8"):
        return True
    type_name = (row.get("TYPE_NAME") or "").strip().lower()
    type_img  = _to_str(row.get("TYPE_IMG")) or ""
    gt_type   = _to_str(row.get("GT_SHIPTYPE")) or ""
    if type_name == "tanker": return True
    if type_img == "8": return True
    if gt_type == "17": return True
    return False

def make_vessel_uid(row: Dict[str, Any]) -> str:
    """
    Construct a stable vessel UID:
      MMSI > IMO > MarineTraffic SHIP_ID (digits) > SAT opaque SHIP_ID (hashed) > hashed surrogate.
    For SAT rows with opaque SHIP_ID, we hash it so each SAT track becomes a distinct vessel_uid.
    """
    mmsi = row.get("MMSI") or row.get("mmsi")
    imo  = row.get("IMO")  or row.get("imo")
    if mmsi:
        try:
            return f"mmsi:{int(mmsi)}"
        except Exception:
            pass
    if imo:
        try:
            return f"imo:{int(imo)}"
        except Exception:
            pass

    ship_id = str(row.get("SHIP_ID") or "").strip()
    if ship_id:
        if ship_id.isdigit():
            # MT numeric id seen on terrestrial feeds: reasonably stable
            return f"mtid:{ship_id}"
        else:
            return f"sat:{ship_id}"

    # Fallback surrogate (for very sparse SAT rows without SHIP_ID)
    name   = _norm(row.get("SHIPNAME"))
    if name in {"[SAT-AIS]", "UNKNOWN"}:
        name = ""
    flag   = _norm(row.get("FLAG"))
    length = _norm(row.get("LENGTH"))
    width  = _norm(row.get("WIDTH"))
    stype  = _norm(row.get("SHIPTYPE") or row.get("TYPE_NAME"))
    parts  = [name, flag, length, width, stype]
    h = hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()
    return f"h:{h[:16]}"



# --------------------------- Optional bbox prefilter ------------------------
# Load buffered bbox extents for area/gate polygons to cheaply reject far-away points.
# This is an optional optimization; DB triggers still do exact membership labeling.
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as e:
    raise

@dataclass
class BBox:
    xmin: float
    ymin: float
    xmax: float
    ymax: float

AREA_BBOXES: List[BBox] = []

def load_area_bboxes() -> None:
    """
    Load buffered bounding boxes for public.area and public.area_gate.
    Uses ST_Envelope and ST_Expand in degrees (WGS84) with PREFETCH_BUFFER_DEG.
    """
    if not ENABLE_BBOX_PREFILTER:
        return
    global AREA_BBOXES
    AREA_BBOXES = []
    sql = f"""
    WITH all_geoms AS (
      SELECT ST_Envelope(ST_Expand(geom, {PREFETCH_BUFFER_DEG})) AS g FROM public.area
      UNION ALL
      SELECT ST_Envelope(ST_Expand(geom, {PREFETCH_BUFFER_DEG})) AS g FROM public.area_gate
    )
    SELECT ST_XMin(g) AS xmin, ST_YMin(g) AS ymin, ST_XMax(g) AS xmax, ST_YMax(g) AS ymax FROM all_geoms;
    """
    try:
        with psycopg.connect(PG_DSN) as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            for r in cur.fetchall():
                AREA_BBOXES.append(BBox(r["xmin"], r["ymin"], r["xmax"], r["ymax"]))
        logger.info("prefilter: loaded %d buffered bboxes", len(AREA_BBOXES))
    except Exception as e:
        logger.exception("prefilter: failed to load bboxes: %s", e)
        AREA_BBOXES = []

def in_any_bbox(lon: float, lat: float) -> bool:
    if not AREA_BBOXES:
        return True  # no prefiltering; let DB handle it
    for b in AREA_BBOXES:
        if b.xmin <= lon <= b.xmax and b.ymin <= lat <= b.ymax:
            return True
    return False

# ------------------------ Backoff fetch wrapper -----------------------------
def fetch_with_backoff(driver, z, x, y, idx, max_retries=MAX_RETRIES) -> Optional[dict]:
    attempt = 0
    while True:
        payload = fetch_once(driver, tile_url(z, x, y), wait_seconds=10)

        ok = bool(payload and payload.get("data", {}).get("rows"))
        if ok:
            if attempt > 0:
                logger.warning(
                    "backoff: tile z:%s x:%s y:%s succeeded after %s attempt(s)",
                    z, x, y, attempt + 1
                )
                ui_set_bottom(f"[BACKOFF] tile z:{z} x:{x} y:{y} succeeded after {attempt+1} attempt(s)")
            return payload or {}

        if attempt >= max_retries:
            ui_set_bottom(f"[BACKOFF] tile z:{z} x:{x} y:{y} exhausted retries {attempt+1}; continuing")

            tile_jitter = random.uniform(-TILE_JITTER_MS, TILE_JITTER_MS)
            time.sleep(max(0.0, (TILE_PAUSE_MS + tile_jitter) / 1000.0))
            
            return payload or {}

        sleep_s = BACKOFF_BASE_SECS * (2 ** attempt)
        ui_set_bottom(f"[BACKOFF] tile z:{z} x:{x} y:{y} attempt {attempt+1} -> sleeping {sleep_s:.1f}s")
        time.sleep(sleep_s)
        attempt += 1

# ------------------------ JSON → normalized rows ----------------------------
def derive_ts(fetch_ts: datetime, elapsed_min_val: Any) -> Optional[datetime]:
    """
    Turn ELAPSED (minutes) into absolute observation time.
    Drop if absurdly old (configurable).
    """
    if elapsed_min_val is None:
        return fetch_ts
    try:
        em = int(str(elapsed_min_val).strip())
    except Exception:
        return fetch_ts
    if em < 0 or em > DROP_IF_OBS_AGE_MIN:
        return None
    return fetch_ts - timedelta(minutes=em)

def normalize_rows_from_payload(payload: Dict[str, Any], tile_id: str) -> List[Dict[str, Any]]:
    """
    Transform MarineTraffic payload (data.rows) into a list of records ready for
    insertion into public.ais_fix. Applies tanker filter, source classification,
    vessel identity creation, SAT continuity, idempotency, and optional bbox prefilter.
    """
    rows = payload.get("data", {}).get("rows", []) if payload else []
    fetch_ts = _utcnow()

    out: List[Dict[str, Any]] = []
    for r in rows:
        # 0) Only tankers
        if not tanker_predicate(r):
            continue

        # 1) ts from ELAPSED
        ts = derive_ts(fetch_ts, r.get("ELAPSED"))
        if ts is None:
            continue

        # 2) coords
        lat = _to_float(r.get("LAT"))
        lon = _to_float(r.get("LON"))
        if lat is None or lon is None:
            continue
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            continue

        # Cheap spatial prefilter to avoid DB inserts far from areas
        if ENABLE_BBOX_PREFILTER and not in_any_bbox(lon, lat):
            continue

        # 3) source & kinematics
        src = classify_src(r)
        if src == "sat":
            # DROP SAT SIGNAL AS TOO JITTERY FOR NOW
            continue

        # MarineTraffic SPEED looks like deciknots; normalize to knots
        sog = _to_float(r.get("SPEED"))
        sog = sog / 10.0 if sog is not None else None
        if sog is not None and sog > MAX_TANKER_SOG_KN:
            # allow zero/low speeds, but drop absurd movers
            continue
        cog = _to_float(r.get("COURSE"))
        heading = _to_int(r.get("HEADING"))
        rot = _to_float(r.get("ROT"))
        elapsed = _to_int(r.get("ELAPSED"))

        # 4) identity & continuity
        vuid = make_vessel_uid(r)

        # 5) project into ais_fix columns (DB triggers will enrich geom/memberships)
        rec: Dict[str, Any] = {
            "ts": ts,
            "src": src,
            "vessel_uid": vuid,
            "lat": lat,
            "lon": lon,
            "sog": sog,
            "cog": cog,
            "heading": heading,
            "elapsed": elapsed,
            "destination": _to_str(r.get("DESTINATION")),
            "flag": _to_str(r.get("FLAG")),
            "length_m": _to_float(r.get("LENGTH")),
            "width_m": _to_float(r.get("WIDTH")),
            "dwt": _to_int(r.get("DWT")),
            "shipname": _to_str(r.get("SHIPNAME")),
            "shiptype": _to_int(r.get("SHIPTYPE")),  # may be None; DB accepts NULL
            "ship_id": _to_str(r.get("SHIP_ID")),    # keep as text for provenance
            "rot": rot
        }
        out.append(rec)

    # (Optional) stable eventization: order by vessel then ts
    out.sort(key=lambda r: (r["vessel_uid"] or "", r["ts"]))
    return out

# ------------------------------ DB I/O --------------------------------------
INSERT_FIX_SQL = """
INSERT INTO public.ais_fix
( ts, src, vessel_uid,
  lat, lon, sog, cog, heading, elapsed,
  destination, flag, length_m, width_m, dwt, shipname, shiptype, ship_id, rot
)
VALUES
( %(ts)s, %(src)s, %(vessel_uid)s,
  %(lat)s, %(lon)s, %(sog)s, %(cog)s, %(heading)s, %(elapsed)s,
  %(destination)s, %(flag)s, %(length_m)s, %(width_m)s, %(dwt)s,
  %(shipname)s, %(shiptype)s, %(ship_id)s, %(rot)s
);
"""

def insert_fixes(rows: List[Dict[str, Any]], batch_size: int = 2000) -> int:
    """
    Bulk insert normalized rows into public.ais_fix.
    Returns count of rows **attempted**; true inserts may be lower due to DB dedupe trigger.
    """
    if not rows:
        return 0
    inserted = 0
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                chunk = rows[i:i+batch_size]
                cur.executemany(INSERT_FIX_SQL, chunk)
                # rowcount reports rows processed; dedupe trigger may drop them silently
                inserted += cur.rowcount if cur.rowcount is not None else len(chunk)
        conn.commit()
    return inserted


from sqlalchemy import create_engine, text
engine = create_engine("postgresql+psycopg2://ais:aispass@localhost:5432/ais", future=True)

def tick_refresh_all(backfill_days: int = 2) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        conn.execute(text("CALL public.tick_refresh_all(:d)"), {"d": backfill_days})

# ------------------------------ Main loop -----------------------------------
def main() -> None:
    global _live_obj

    load_area_bboxes()  # no-op unless ENABLE_BBOX_PREFILTER=1

    driver = open_driver(minimized=True, size=(400, 300))
    tile_iter, total_tiles = build_tile_cycle()

    setup_msg = f"[SETUP] tiles={total_tiles} | per_cycle={TILES_PER_CYCLE} | interval={INTERVAL_SECONDS}s"
    ui_set_setup(setup_msg)
    ui_set_latest("(waiting for first cycle)")

    try:
        if USE_RICH:
            with Live(_renderable(), console=console, refresh_per_second=10, transient=True) as live:
                _run_loop(driver, tile_iter, total_tiles)
        else:
            _refresh()
            _run_loop(driver, tile_iter, total_tiles)
    except KeyboardInterrupt:
        pass
    finally:
        ui_disable()
        print("[INFO] stopping collector.")
        try: safe_quit(driver)
        except: pass
        logger.info("browser closed")
        print("[INFO] browser closed.")

def _run_loop(driver, tile_iter, total_tiles):
    import time
    from itertools import islice
    from random import shuffle, uniform

    cycles = 0

    while True:
        now = time.time()
        t0 = time.monotonic()
        received_total = 0  # raw rows from payloads
        kept_total = 0      # rows kept after normalization/filters
        inserted_total = 0  # attempted inserts (DB may dedupe)

        # recycle webdriver periodically
        if cycles % CYCLES_PER_DRIVER == 0 and cycles != 0:
            try:
                safe_quit(driver)
            except Exception:
                pass
            driver = open_driver(minimized=True, size=(400, 300))
            logger.info("recycled browser")


        # pull the next batch for this cycle
        tiles_this_cycle = list(islice(tile_iter, TILES_PER_CYCLE))
        if not tiles_this_cycle:
            # nothing in this slice, move to next loop
            cycles += 1
            continue
      
        shuffle(tiles_this_cycle)
        failed_tiles = 0

        for idx, (z, x, y) in enumerate(tiles_this_cycle, start=1):
            tile_id = f"{z}, {x}, {y}"

            payload = fetch_with_backoff(driver, z, x, y, idx, max_retries=MAX_RETRIES)
            ok = bool(payload and payload.get("data", {}).get("rows"))
            if not ok:
                failed_tiles += 1
                continue

            rows_raw = payload.get("data", {}).get("rows", []) if payload else []
            received_total += len(rows_raw)

            batch = normalize_rows_from_payload(payload, tile_id)
            kept_total += len(batch)
            ins = insert_fixes(batch)
            inserted_total += ins



            ui_set_bottom(
                f"[FETCH] tile {idx}/{TILES_PER_CYCLE} — z:{z} x:{x} y:{y} "
                f"(recv={len(rows_raw)} kept={len(batch)} ins={ins})"
            )

            # gentle pacing between tiles
            tile_jitter = random.uniform(-TILE_JITTER_MS, TILE_JITTER_MS)
            time.sleep(max(0.0, (TILE_PAUSE_MS + tile_jitter) / 1000.0))
        
        cycles+=1

        # Refresh Caggs/MVs
        from maintenance import refresh_all
        refresh_all(backfill_days=2)

        # per-cycle summary
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary = (f"[LATEST] {now_str} | received={received_total} | kept={kept_total} | "
                   f"inserted={inserted_total} | (tiles this cycle={TILES_PER_CYCLE-failed_tiles}/{TILES_PER_CYCLE})")
        ui_set_latest(summary)
        logger.info("cycle summary: received=%s kept=%s inserted=%s tiles=%s/%s",
                    received_total, kept_total, inserted_total, TILES_PER_CYCLE-failed_tiles, TILES_PER_CYCLE)


        # backoff/cooldown if many tiles failed; optionally recycle session
        cooldown_jitter = int(random.uniform(0, COOLDOWN_JITTER))
        COOLDOWN_TIME = COOLDOWN_SECONDS + cooldown_jitter
        fail_ratio = failed_tiles / max(1, TILES_PER_CYCLE)

        if fail_ratio >= COOLDOWN_FAIL_RATIO:
            if fail_ratio >= REOPEN_FAIL_RATIO:
                logger.warning(
                    "session_recycle_triggered: fail_ratio=%.2f (failed=%d of %d) → sleeping %ds",
                    fail_ratio, failed_tiles, TILES_PER_CYCLE, COOLDOWN_TIME
                )
                ui_set_bottom(
                    f"[RECYCLE] fail_ratio={fail_ratio:.2f} (failed={failed_tiles} of {TILES_PER_CYCLE}) → sleeping {COOLDOWN_TIME}s"
                )
                try: driver.quit()
                except Exception as e: logger.exception("driver_quit_error: %s", e)
                if COOLDOWN_TIME > 0:
                    time.sleep(COOLDOWN_TIME)
                driver = open_driver(minimized=True, size=(400, 300))
                logger.info("session_recycle_complete")
                continue

            logger.warning("cooldown: fail_ratio=%.2f (failed=%d of %d) → sleeping %ds",
                           fail_ratio, failed_tiles, TILES_PER_CYCLE, COOLDOWN_TIME)

            ui_set_bottom(
                f"[COOLDOWN] fail_ratio={fail_ratio:.2f} (failed={failed_tiles} of {TILES_PER_CYCLE}) → sleeping {COOLDOWN_TIME}s"
            )
            time.sleep(COOLDOWN_SECONDS)


        # countdown to next cycle (with jitter)
        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, INTERVAL_SECONDS - elapsed)
        jitter = random.uniform(-JITTER_SECONDS, JITTER_SECONDS)
        sleep_for = max(0.0, sleep_for + jitter)

        if sleep_for > 0:
            deadline = time.time() + sleep_for
            while True:
                rem = int(round(deadline - time.time()))
                if rem <= 0:
                    ui_set_bottom("[NEXT] starting new cycle...")
                    time.sleep(0.3)
                    break
                ui_set_bottom(f"[NEXT] next cycle in {rem:>3d}s")
                time.sleep(1)

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()




