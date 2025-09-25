#!/usr/bin/env python3
"""
build_tiles.py — Generate STATIC_TILES (MarineTraffic z/X/Y) from PostGIS areas & gates.

Adjustment for MT vs OSM tiles:
- Observed mapping: MT tile at (z, x_mt, y_mt) spans a 2×2 block of OSM XYZ tiles at *the same z*:
    OSM tiles = {(z, 2*x_mt + dx, 2*y_mt + dy) | dx,dy ∈ {0,1}}
  Therefore, to cover a bbox:
    1) Compute OSM tile range at zoom z.
    2) Compress indices by factor 2 (floor-divide x,y by 2) to get MT tiles.
    3) Deduplicate.

If your deployment later discovers a different mapping, change MT_XY_FACTOR or
add flags to invert Y or offset zoom. Defaults match your finding: factor=2,
no Y inversion, no zoom offset.

Usage:
  python build_tiles.py \
    --pg-dsn postgresql://ais:aispass@localhost:5432/ais \
    --area-buffer-deg 0.20 \
    --gate-buffer-deg 0.30 \
    --zoom-port-core 7 \
    --zoom-port-approach 6 \
    --zoom-lane-corridor 5 \
    --zoom-chokepoint-corridor 6 \
    --zoom-sts-corridor 6 \
    --zoom-gate 6 \
    [--per-area]

Output:
- One semicolon-separated line: "z/x/y;z/x/y;..."
- Optional per-area breakdown with --per-area.
"""

import os
import math
import argparse
import psycopg
from psycopg.rows import dict_row
from typing import List, Tuple, Set, Dict

MERCATOR_LAT_MAX = 85.05112878   # Web-Mercator clamp
MT_XY_FACTOR     = 2             # MT tile spans 2×2 OSM tiles at same z (your observed mapping)

# ---------- OSM helpers ----------
def lonlat_to_osm_tile(lon: float, lat: float, z: int) -> Tuple[int, int]:
    """OSM XYZ: convert lon/lat to tile x,y at zoom z."""
    lat = max(-MERCATOR_LAT_MAX, min(MERCATOR_LAT_MAX, float(lat)))
    n = 2 ** z
    x = int((float(lon) + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(math.radians(lat)) + 1.0 / math.cos(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y

def osm_tiles_for_bbox(minlon: float, minlat: float, maxlon: float, maxlat: float, z: int) -> List[Tuple[int,int,int]]:
    """
    Return list of OSM (z,x,y) tiles covering bbox at zoom z.
    Handles antimeridian-crossing bboxes by splitting at 180/-180.
    """
    tiles: List[Tuple[int,int,int]] = []

    def cover_segment(lo_lon: float, hi_lon: float):
        x0, y1 = lonlat_to_osm_tile(lo_lon, minlat, z)  # SW
        x1, y0 = lonlat_to_osm_tile(hi_lon, maxlat, z)  # NE
        xs = range(min(x0, x1), max(x0, x1) + 1)
        ys = range(min(y0, y1), max(y0, y1) + 1)
        for x in xs:
            for y in ys:
                tiles.append((z, x, y))

    # Normalize longitudes to [-180, 180]
    minlon = ((minlon + 180.0) % 360.0) - 180.0
    maxlon = ((maxlon + 180.0) % 360.0) - 180.0

    if maxlon >= minlon:
        cover_segment(minlon, maxlon)
    else:
        # Crosses antimeridian: split into [minlon,180] ∪ [-180,maxlon]
        cover_segment(minlon, 180.0 - 1e-9)
        cover_segment(-180.0, maxlon)

    return tiles

# ---------- OSM → MarineTraffic compression ----------
def compress_osm_to_mt(osm_tiles: List[Tuple[int,int,int]], factor: int = MT_XY_FACTOR) -> Set[Tuple[int,int,int]]:
    """
    Map OSM tiles to MT tiles by floor-dividing x,y by 'factor' (default 2).
    Deduplicates automatically via a set.
    """
    out: Set[Tuple[int,int,int]] = set()
    for z, x, y in osm_tiles:
        out.add((z, x // factor, y // factor))
    return out

# ---------- Zoom selection ----------
def pick_zoom(kind: str, subtype: str, is_gate: bool, args) -> int:
    """Choose a 'MarineTraffic zoom' per class (same z used for OSM coverage before compressing)."""
    if is_gate:
        return args.zoom_gate
    if kind == "port" and subtype == "core":
        return args.zoom_port_core
    if kind == "port" and subtype == "approach":
        return args.zoom_port_approach
    if subtype == "corridor":
        if kind == "lane":
            return args.zoom_lane_corridor
        if kind == "chokepoint":
            return args.zoom_chokepoint_corridor
        if kind == "sts":
            return args.zoom_sts_corridor
    return args.zoom_lane_corridor

# ---------- DB fetch ----------
def fetch_area_bboxes(conn, buffer_deg: float) -> List[dict]:
    sql = """
    SELECT area_id, kind, subtype,
           ST_XMin(env) AS minlon, ST_YMin(env) AS minlat,
           ST_XMax(env) AS maxlon, ST_YMax(env) AS maxlat
    FROM (
      SELECT area_id, kind, subtype, ST_Envelope(ST_Expand(geom, %s)) AS env
      FROM public.area
    ) q;
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (buffer_deg,))
        return cur.fetchall()

def fetch_gate_bboxes(conn, buffer_deg: float) -> List[dict]:
    sql = """
    SELECT gate_id, area_id, kind, subtype,
           ST_XMin(env) AS minlon, ST_YMin(env) AS minlat,
           ST_XMax(env) AS maxlon, ST_YMax(env) AS maxlat
    FROM (
      SELECT gate_id, area_id, kind, subtype, ST_Envelope(ST_Expand(geom, %s)) AS env
      FROM public.area_gate
    ) q;
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (buffer_deg,))
        return cur.fetchall()

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Build MarineTraffic STATIC_TILES from PostGIS areas/gates (MT grid = OSM grid compressed by 2x).")
    ap.add_argument("--pg-dsn", default=os.getenv("PG_DSN", "postgresql://ais:aispass@localhost:5432/ais"))
    ap.add_argument("--area-buffer-deg", type=float, default=0.20, help="degree buffer for areas (ports/lanes/etc.)")
    ap.add_argument("--gate-buffer-deg", type=float, default=0.30, help="degree buffer for gates (make them 'fat')")
    # MarineTraffic zooms per class (used for OSM coverage, then compressed to MT)
    ap.add_argument("--zoom-port-core", type=int, default=7)
    ap.add_argument("--zoom-port-approach", type=int, default=6)
    ap.add_argument("--zoom-lane-corridor", type=int, default=5)
    ap.add_argument("--zoom-chokepoint-corridor", type=int, default=6)
    ap.add_argument("--zoom-sts-corridor", type=int, default=6)
    ap.add_argument("--zoom-gate", type=int, default=6)
    # Mapping factor (OSM→MT); leave at 2 for your case
    ap.add_argument("--mt-xy-factor", type=int, default=MT_XY_FACTOR, help="OSM tiles per MT tile edge (default 2)")
    ap.add_argument("--per-area", action="store_true", help="also print per-area groups for debugging")
    args = ap.parse_args()

    factor = max(1, int(args.mt_xy_factor))

    mt_tiles: Set[Tuple[int,int,int]] = set()
    per_area: Dict[str, Set[Tuple[int,int,int]]] = {}

    with psycopg.connect(args.pg_dsn) as conn:
        # Areas
        for r in fetch_area_bboxes(conn, args.area_buffer_deg):
            z = pick_zoom(r["kind"], r["subtype"], False, args)
            osm = osm_tiles_for_bbox(r["minlon"], r["minlat"], r["maxlon"], r["maxlat"], z)
            mt  = compress_osm_to_mt(osm, factor=factor)
            mt_tiles |= mt
            if args.per_area:
                per_area.setdefault(r["area_id"], set()).update(mt)

        # Gates
        for r in fetch_gate_bboxes(conn, args.gate_buffer_deg):
            z = pick_zoom(r["kind"], "gate", True, args)
            osm = osm_tiles_for_bbox(r["minlon"], r["minlat"], r["maxlon"], r["maxlat"], z)
            mt  = compress_osm_to_mt(osm, factor=factor)
            mt_tiles |= mt
            if args.per_area:
                label = f"{r['area_id']}::{r['gate_id']}"
                per_area.setdefault(label, set()).update(mt)

    # Stable sort and print
    out = sorted(mt_tiles, key=lambda t: (t[0], t[1], t[2]))
    s = ";".join(f"{z}/{x}/{y}" for (z,x,y) in out)

    print("# Paste this into STATIC_TILES (MarineTraffic z/X/Y; semicolon-separated):")
    print(s)
    print(f"\n# total MT tiles: {len(out)}")

    if args.per_area:
        print("\n# ---- Per-area MT tile breakdown ----")
        for k in sorted(per_area.keys()):
            lst = sorted(per_area[k], key=lambda t: (t[0], t[1], t[2]))
            joined = ";".join(f"{z}/{x}/{y}" for (z,x,y) in lst)
            print(f"# {k} ({len(lst)} MT tiles):")
            print(joined)
            print()

if __name__ == "__main__":
    main()
