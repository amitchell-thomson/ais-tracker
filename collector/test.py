#!/usr/bin/env python3
"""
osm2mt_candidates.py — Given an OSM/XYZ tile (z, x, y), print possible
MarineTraffic station:0 tile URLs that might correspond to it.

We try 4 common mappings:
  1) MT XYZ,   same zoom
  2) MT TMS,   same zoom (invert Y)
  3) MT XYZ,   z-1 (x//2, y//2)
  4) MT TMS,   z-1 (x//2, invert Y at z-1)

Use the printed URLs to click/compare and see which one lines up for your setup.
"""

from __future__ import annotations
import argparse

def invert_y_tms(z: int, y_xyz: int) -> int:
    """XYZ ↔ TMS Y conversion at zoom z."""
    return (2**z - 1) - y_xyz

def mt_url(z: int, x: int, y: int, station: int = 0) -> str:
    return f"https://www.marinetraffic.com/getData/get_data_json_4/z:{z}/X:{x}/Y:{y}/station:{station}"

def main():
    ap = argparse.ArgumentParser(description="OSM (XYZ) tile -> MarineTraffic station:0 candidate URLs")
    ap.add_argument("--z", type=int, required=True, help="OSM zoom")
    ap.add_argument("--x", type=int, required=True, help="OSM x")
    ap.add_argument("--y", type=int, required=True, help="OSM y")
    ap.add_argument("--station", type=int, default=0, help="MarineTraffic station id (default 0)")
    args = ap.parse_args()

    z, x, y = args.z, args.x, args.y
    print(f"# OSM XYZ: {z}/{x}/{y}\n")

    # 1) XYZ same zoom
    print("[A] MT XYZ, same zoom")
    print(mt_url(z, x, y, args.station), "\n")

    # 2) TMS same zoom (invert Y at same z)
    y_tms_same = invert_y_tms(z, y)
    print("[B] MT TMS, same zoom (Y inverted)")
    print(mt_url(z, x, y_tms_same, args.station), "\n")

    # 3) XYZ z-1 (halve indices)
    if z > 0:
        z_minus = z - 1
        x_half = x // 2
        y_half = y // 2
        print("[C] MT XYZ, z-1 (x//2, y//2)")
        print(mt_url(z_minus, x_half, y_half, args.station), "\n")

        # 4) TMS z-1
        y_tms_zminus = invert_y_tms(z_minus, y_half)
        print("[D] MT TMS, z-1 (x//2, invert Y at z-1)")
        print(mt_url(z_minus, x_half, y_tms_zminus, args.station), "\n")
    else:
        print("# (z=0 has no z-1 candidates)")

    # Optional: show the OSM raster for quick visual compare
    print("# OSM PNG (for reference):")
    print(f"https://tile.openstreetmap.org/{z}/{x}/{y}.png")

if __name__ == "__main__":
    main()
