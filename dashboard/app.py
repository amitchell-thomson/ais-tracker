# dashboard/app.py
from __future__ import annotations

import os
from typing import Dict, Any, Optional, List

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine
from datetime import datetime

# -----------------------------
# Config
# -----------------------------
st.set_page_config(
    page_title="AIS Ports Dashboard",
    page_icon="🛳️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Light theme accents
st.markdown("""
<style>
.reportview-container .main { background-color: #f8fafc; }
.block-container { padding-top: 1rem; padding-bottom: 2rem; }
</style>
""", unsafe_allow_html=True)

DEFAULT_DSN = "postgresql://ais:aispass@localhost:5432/ais"
PG_DSN = st.secrets.get("PG_DSN", os.getenv("PG_DSN", DEFAULT_DSN))

# -----------------------------
# DB Layer
# -----------------------------
@st.cache_resource
def get_engine() -> Engine:
    return create_engine(
        PG_DSN,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
        future=True,
    )

def fetch_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET TIME ZONE 'UTC'"))
        return pd.read_sql_query(text(sql), conn, params=params)

def q(sql: str, **params) -> pd.DataFrame:
    return fetch_df(sql, params)

# -----------------------------
# Reusable SQL (new schema)
# -----------------------------

# Overview (now on ais_fix)
SQL_OVERVIEW = """
WITH latest_ts AS (
  SELECT max(ts) AS last_ts FROM public.ais_fix
),
unique_vessels AS (
  SELECT count(DISTINCT vessel_uid) AS uniq FROM public.ais_fix
),
recent_snapshot AS (
  SELECT DISTINCT ON (vessel_uid)
         vessel_uid, ts,
         (area_id_core IS NOT NULL) AS in_core,
         (area_id_approach IS NOT NULL AND area_id_core IS NULL) AS in_approach
  FROM public.ais_fix
  WHERE ts >= now() - make_interval(hours => :hours)
  ORDER BY vessel_uid, ts DESC
)
SELECT
  (SELECT uniq FROM unique_vessels)                 AS unique_vessels,
  (SELECT last_ts FROM latest_ts)                   AS last_ts,
  sum((in_core)::int)                               AS vessels_in_port,
  sum((in_approach)::int)                           AS vessels_in_approach
FROM recent_snapshot;
"""

SQL_AREAS_GEOJSON = """
SELECT
  area_id, name, kind, subtype, "group",
  ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, :tol)) AS geojson
FROM public.area
"""

SQL_GATES_GEOJSON = """
SELECT
  gate_id, area_id, name, kind, subtype, "group",
  ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, :tol)) AS geojson
FROM public.area_gate
"""



SQL_INGEST_RATE = """
SELECT date_trunc('minute', ts) AS minute, count(*) AS rows_inserted
FROM public.ais_fix
WHERE ts >= now() - make_interval(hours => :hours)
GROUP BY 1
ORDER BY 1;
"""

# Occupancy snapshot (group by area polygons you’ve defined)
SQL_OCCUPANCY_SNAPSHOT = """
WITH latest AS (
  SELECT DISTINCT ON (vessel_uid)
         vessel_uid,
         area_id_core,
         area_id_approach,
         ts
  FROM public.ais_fix
  WHERE area_id_core IS NOT NULL OR area_id_approach IS NOT NULL
  ORDER BY vessel_uid, ts DESC
)
SELECT
  coalesce(area_id_core, area_id_approach) AS area_id,
  sum((area_id_core IS NOT NULL)::int)     AS vessels_in_core,
  sum((area_id_approach IS NOT NULL AND area_id_core IS NULL)::int) AS vessels_in_approach
FROM latest
GROUP BY 1
ORDER BY 2 DESC, 3 DESC, 1;
"""

# Areas & lanes lists from your seed tables
SQL_AREA_PORTS = """
SELECT area_id, name
FROM public.area
WHERE kind='port' AND subtype in ('core','approach')
ORDER BY name, area_id;
"""

SQL_LANES = """
SELECT area_id AS lane_id, name
FROM public.area
WHERE kind in ('lane','chokepoint')
ORDER BY name, area_id;
"""

# Latest fixes for map
SQL_LATEST_FIXES = """
SELECT DISTINCT ON (vessel_uid)
  vessel_uid, ts, lat, lon, sog, cog, heading,
  shipname, shiptype, flag, length_m, width_m,
  area_id_core, area_id_approach, lane_id
FROM public.ais_fix
ORDER BY vessel_uid, ts DESC;
"""


# ---- CAGGs / EWMs (new features) ----

# Port lifts (daily raw cagg + ewm)
SQL_CA_PORT_LIFTS_DAILY = """
SELECT day, area_id, arrive_dwt, depart_dwt
FROM public.ca_port_lifts_daily
WHERE area_id = :area_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

SQL_CA_PORT_LIFTS_EWM = """
SELECT day, area_id, arrive_dwt_ewm, depart_dwt_ewm
FROM public.ca_port_lifts_ewm
WHERE area_id = :area_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

# Lane transit (ewm)
SQL_CA_LANE_EWM = """
SELECT day, lane_id, transit_cnt_ewm, laden_dwt_sum_ewm
FROM public.ca_lane_transit_ewm
WHERE lane_id = :lane_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

# Transit time (mv + ewm)
SQL_MV_LANE_TT = """
SELECT day, lane_id, p50_hours, p90_hours, mean_hours
FROM public.mv_lane_transit_time_daily
WHERE lane_id = :lane_id AND day BETWEEN :start AND :end
ORDER BY day;
"""
SQL_CA_TT_EWM = """
SELECT day, lane_id, transit_time_p50_ewm
FROM public.ca_transit_time_ewm
WHERE lane_id = :lane_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

# Anchorage queue (mv + ewm) — uses area_id (approaches)
SQL_MV_ANCHOR = """
SELECT day, area_id, avg_cnt, p50_cnt
FROM public.mv_area_occupancy_daily
WHERE area_id = :area_id AND day BETWEEN :start AND :end
ORDER BY day;
"""
SQL_CA_ANCHOR_EWM = """
SELECT day, area_id, queue_cnt_ewm
FROM public.ca_anchorage_queue_ewm
WHERE area_id = :area_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

# Ballast returns (ewm)
SQL_CA_BALLAST_EWM = """
SELECT day, lane_id, ballast_dwt_ewm
FROM public.ca_ballast_return_ewm
WHERE lane_id = :lane_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

# Class mix (ewm)
SQL_CA_CLASS_MIX = """
SELECT day, lane_id, vlcc_share_ewm, suezmax_share_ewm, aframax_share_ewm
FROM public.ca_class_mix_ewm
WHERE lane_id = :lane_id AND day BETWEEN :start AND :end
ORDER BY day;
"""

# Final features (if built)
SQL_ML_FEATURES = """
SELECT *
FROM public.ml_features_daily
WHERE day BETWEEN :start AND :end
ORDER BY day;
"""

# -----------------------------
# Small helpers
# -----------------------------
def header_kpis(key_prefix: str = "default"):
    st.sidebar.subheader("Dashboard Refresh")
    hours = st.sidebar.slider(
        "Overview lookback (hours)",
        min_value=1, max_value=48, value=24, step=1,
        key=f"{key_prefix}_overview_hours"
    )
    map_age = st.sidebar.slider(
        "Map lookback (hours)",
        min_value=1, max_value=48, value=1, step=1,
        key=f"{key_prefix}_map_age"
    )
    return hours, map_age

def friendly_ts(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    return pd.to_datetime(ts).strftime("%H:%M:%S")

def list_port_area_ids() -> List[str]:
    try:
        df = fetch_df(SQL_AREA_PORTS)
        return df["area_id"].tolist()
    except Exception:
        return []

def list_lane_ids() -> List[str]:
    try:
        df = fetch_df(SQL_LANES)
        return df["lane_id"].tolist()
    except Exception:
        return []

# -----------------------------
# Tabs (original + new analytics)
# -----------------------------
def tab_overview(hours):
    st.markdown("## 🧭 Overview")

    cols = st.columns([1.5,1.5,1.5,1.5])
    try:
        df = fetch_df(SQL_OVERVIEW, {"hours": hours})
        unique_vessels = int(df.iloc[0]["unique_vessels"]) if not df.empty else 0
        last_ts = df.iloc[0]["last_ts"] if not df.empty else None
        in_port = int(df.iloc[0]["vessels_in_port"]) if not df.empty else 0
        in_appr = int(df.iloc[0]["vessels_in_approach"]) if not df.empty else 0
    except Exception as e:
        st.error(f"Could not load overview: {e}")
        unique_vessels, last_ts, in_port, in_appr = 0, None, 0, 0

    cols[0].metric("Total unique vessels (all time)", f"{unique_vessels:,}")
    cols[1].metric("Last AIS timestamp", friendly_ts(last_ts))
    cols[2].metric("Vessels in port (recent snapshot)", f"{in_port:,}")
    cols[3].metric("Vessels in approach (recent snapshot)", f"{in_appr:,}")

    st.markdown("---")
    st.subheader("Ingestion rate (rows/min)")
    try:
        rate = fetch_df(SQL_INGEST_RATE, {"hours": hours})
        if rate.empty:
            st.info("No data in the selected lookback.")
        else:
            rate["minute"] = pd.to_datetime(rate["minute"], utc=True)
            rate = rate.set_index("minute")
            st.line_chart(rate["rows_inserted"])
    except Exception as e:
        st.error(f"Could not load ingestion chart: {e}")

    st.markdown("---")
    st.subheader("Current port/approach snapshot")
    try:
        snap = fetch_df(SQL_OCCUPANCY_SNAPSHOT)
        if snap.empty:
            st.info("No vessels currently inside any configured areas.")
        else:
            st.dataframe(
                snap.rename(columns={
                    "area_id": "Area",
                    "vessels_in_core": "In Port (core)",
                    "vessels_in_approach": "In Approach"
                }),
                width='stretch', height=360
            )
    except Exception as e:
        st.error(f"Could not load occupancy snapshot: {e}")

def tab_ports():
    st.markdown("## 🏗️ Ports & Occupancy (ad-hoc)")

    # Pick a port core/approach area_id
    areas = fetch_df(SQL_AREA_PORTS)
    if areas.empty:
        st.warning("No port areas found. Seed `public.area` first.")
        return

    options = list(areas["area_id"])
    labels = list(areas["name"] + " — " + areas["area_id"])
    choice = st.selectbox("Choose a port area", options=list(zip(labels, options)), format_func=lambda t: t[0])
    area_id = choice[1]
    days = st.slider("History window (days)", min_value=1, max_value=30, value=7, step=1)

    # If you later add an occupancy CAGG, call it here; for now do on-the-fly 5-min buckets
    sql_fallback = """
    SELECT time_bucket('5 minutes', ts) AS bucket,
           count(DISTINCT vessel_uid) FILTER (WHERE area_id_core = :aid) AS in_core,
           count(DISTINCT vessel_uid) FILTER (WHERE area_id_approach = :aid AND area_id_core IS NULL) AS in_approach
    FROM public.ais_fix
    WHERE ts >= now() - make_interval(days => :days)
      AND (:aid = :aid) -- binder
    GROUP BY 1
    ORDER BY 1;
    """
    series = fetch_df(sql_fallback, {"aid": area_id, "days": days})
    if series.empty:
        st.info("No occupancy data for this window.")
        return

    series["bucket"] = pd.to_datetime(series["bucket"], utc=True)
    series = series.sort_values("bucket")
    st.write(f"**Time series occupancy (5-min buckets)** — {area_id}")
    st.line_chart(series.set_index("bucket")[["in_core", "in_approach"]], width='stretch', height=280)

def tab_map(map_age):
    import json
    import folium
    from streamlit.components.v1 import html as st_html  # linter may warn; runtime is fine

    st.markdown("## 🗺️ Live Map")

    # New: choose how many recent fixes per vessel to draw (1..5)
    top_n = st.slider("Recent fixes per vessel", 1, 5, 5, step=1)

    # --- Data ---
    if top_n == 1:
        # retain your existing single-latest-per-vessel source
        fixes = fetch_df(SQL_LATEST_FIXES)
    else:
        # inline SQL to fetch last N fixes per vessel (ranked)
        SQL_LATEST_N_FIXES = """
        WITH ranked AS (
          SELECT
            vessel_uid, ts, lat, lon, sog, cog, heading,
            shipname, shiptype, flag, length_m, width_m,
            area_id_core, in_core, area_id_approach, in_approach, lane_id, in_lane,
            ROW_NUMBER() OVER (PARTITION BY vessel_uid ORDER BY ts DESC) AS rn
          FROM public.ais_fix
          WHERE ts >= now() - make_interval(hours => :hours)
        )
        SELECT *
        FROM ranked
        WHERE rn <= :n
        ORDER BY vessel_uid, rn DESC;
        """
        # use the same lookback window as your page control
        fixes = fetch_df(SQL_LATEST_N_FIXES, {"hours": int(map_age or 12), "n": int(top_n)})

    # simplify polygons for speed (tune 0.002–0.02)
    areas_poly = fetch_df(SQL_AREAS_GEOJSON, {"tol": 0.01})
    gates_poly = fetch_df(SQL_GATES_GEOJSON, {"tol": 0.01})

    # Filter fixes by lookback (still applied for the top_n == 1 path)
    if not fixes.empty and "ts" in fixes.columns:
        fixes["ts"] = pd.to_datetime(fixes["ts"], utc=True, errors="coerce")
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=int(map_age or 12))
        fixes = fixes[fixes["ts"] >= cutoff]
    for col in ("lat", "lon"):
        if col in fixes.columns:
            fixes[col] = pd.to_numeric(fixes[col], errors="coerce")
    fixes = fixes.dropna(subset=["lat", "lon"]) if not fixes.empty else fixes

    # --- Base map (initial default; JS will override with persisted view) ---
    m = folium.Map(
        location=[35.0, -30.0],  # mid-Atlantic default; your persisted view will replace this
        zoom_start=3,
        tiles="CartoDB positron",
        prefer_canvas=True,
        control_scale=True,
        zoom_control=True,
    )

    # ---- Style helpers ----
    def area_style(feat):
        k = (feat["properties"].get("kind") or "").lower()
        sub = (feat["properties"].get("subtype") or "").lower()
        if k == "port" and sub == "core":
            return {"color": "#8e44ad", "weight": 2, "fillColor": "#8e44ad", "fillOpacity": 0.10}
        if k == "port" and sub == "approach":
            return {"color": "#2980b9", "weight": 2, "fillColor": "#2980b9", "fillOpacity": 0.06}
        if k == "lane":
            return {"color": "#e67e22", "weight": 2, "fillColor": "#e67e22", "fillOpacity": 0.05}
        if k == "chokepoint":
            return {"color": "#c0392b", "weight": 2, "fillColor": "#c0392b", "fillOpacity": 0.05}
        if k == "sts":
            return {"color": "#27ae60", "weight": 2, "fillColor": "#27ae60", "fillOpacity": 0.05}
        return {"color": "#7f8c8d", "weight": 1, "fillColor": "#7f8c8d", "fillOpacity": 0.04}

    def gate_style(_feat):
        return {"color": "#111111", "weight": 3, "fillOpacity": 0.0}

    # --- Areas polygons ---
    if not areas_poly.empty:
        fg_areas = folium.FeatureGroup(name="Areas (polygons)", show=True)
        for _, r in areas_poly.iterrows():
            try:
                gj = json.loads(r["geojson"])
            except Exception:
                continue
            props = {
                "area_id": r["area_id"],
                "name": r["name"],
                "kind": r["kind"],
                "subtype": r["subtype"],
                "group": r.get("group"),
            }
            folium.GeoJson(
                data={"type": "Feature", "geometry": gj, "properties": props},
                style_function=area_style,
                tooltip=folium.GeoJsonTooltip(
                    fields=["name", "kind", "subtype", "area_id", "group"],
                    aliases=["Name", "Kind", "Subtype", "Area ID", "Group"],
                    sticky=True,
                ),
            ).add_to(fg_areas)
        fg_areas.add_to(m)

    # --- Gates polygons ---
    if not gates_poly.empty:
        fg_gates = folium.FeatureGroup(name="Gates", show=True)
        for _, r in gates_poly.iterrows():
            try:
                gj = json.loads(r["geojson"])
            except Exception:
                continue
            props = {
                "gate_id": r["gate_id"],
                "area_id": r["area_id"],
                "name": r["name"],
                "kind": r["kind"],
                "subtype": r["subtype"],
                "group": r.get("group"),
            }
            folium.GeoJson(
                data={"type": "Feature", "geometry": gj, "properties": props},
                style_function=gate_style,
                tooltip=folium.GeoJsonTooltip(
                    fields=["name", "kind", "subtype", "gate_id", "area_id"],
                    aliases=["Name", "Kind", "Subtype", "Gate ID", "Parent Area"],
                    sticky=True,
                ),
            ).add_to(fg_gates)
        fg_gates.add_to(m)

    # --- Ships layer ---
    def color_and_radius(row):
        stype = str(row.get("shiptype") or "")
        color = "#e74c3c" if stype.startswith("8") else "#7f8c8d"
        return color, 1200

    if not fixes.empty:
        if top_n == 1:
            # Original single-point behavior (keeps clustering threshold)
            if len(fixes) > 12000:
                from folium.plugins import FastMarkerCluster
                pts = [
                    [float(r["lat"]), float(r["lon"]), (r.get("shipname") or r.get("vessel_uid") or "Vessel")]
                    for _, r in fixes.iterrows()
                    if pd.notnull(r.get("lat")) and pd.notnull(r.get("lon"))
                ]
                FastMarkerCluster(data=pts, name="Ships (clustered)").add_to(m)
            else:
                for _, r in fixes.iterrows():
                    lat, lon = float(r["lat"]), float(r["lon"])
                    color, radius_m = color_and_radius(r)
                    name = str(r.get("shipname") or r.get("vessel_uid") or "Vessel")
                    sog = r.get("sog", "")
                    popup_html = f"""
                        <b>{name}</b><br/>
                        UID: {r.get('vessel_uid','')}<br/>
                        ts: {r.get('ts','')}<br/>SOG: {sog} kn | CoG: {r.get('cog','')}<br/>
                        core: {r.get('area_id_core','')}<br/>
                        appr: {r.get('area_id_approach','')}<br/>
                        lane: {r.get('lane_id','')}
                    """
                    folium.Circle(
                        location=[lat, lon],
                        radius=radius_m,
                        color=color, weight=1, fill=True, fill_color=color, fill_opacity=0.8,
                        tooltip=name, popup=folium.Popup(popup_html, max_width=320),
                    ).add_to(m)
        else:
            # Plot last N fixes per vessel with a small trail
            fg_tracks = folium.FeatureGroup(name=f"Tracks (last {top_n})", show=True)
            fg_points = folium.FeatureGroup(name="Fixes", show=True)

            for vuid, g in fixes.groupby("vessel_uid"):
                g = g.sort_values("ts")  # oldest→newest
                pts = list(zip(g["lat"].tolist(), g["lon"].tolist()))
                if len(pts) >= 2:
                    # color by most-recent point’s type
                    color, _ = color_and_radius(g.iloc[-1])
                    folium.PolyLine(
                        locations=pts,
                        color=color,
                        weight=2,
                        opacity=0.7,
                    ).add_to(fg_tracks)

                # fade older points (newest brightest)
                has_rn = "rn" in g.columns
                max_rn = int(g["rn"].max()) if has_rn else len(g)
                for _, r in g.iterrows():
                    color, _ = color_and_radius(r)
                    rn = int(r["rn"]) if has_rn else max_rn  # 1=newest
                    opacity = 0.4 + 0.5 * (max(0, (max_rn - rn)) / max(1, max_rn - 1))
                    name = str(r.get("shipname") or r.get("vessel_uid") or "Vessel")
                    sog = r.get("sog", "")
                    popup_html = f"""
                        <b>{name}</b><br/>
                        UID: {r.get('vessel_uid','')}<br/>
                        ts: {r.get('ts','')}<br/>SOG: {sog} kn | CoG: {r.get('cog','')}<br/>
                        core: {r.get('area_id_core','')}<br/>
                        appr: {r.get('area_id_approach','')}<br/>
                        lane: {r.get('lane_id','')}
                    """
                    folium.Circle(
                        location=[float(r["lat"]), float(r["lon"])],
                        radius=900,
                        color=color,
                        weight=1,
                        fill=True,
                        fill_color=color,
                        fill_opacity=float(opacity),
                        tooltip=name,
                        popup=folium.Popup(popup_html, max_width=320),
                    ).add_to(fg_points)

            fg_tracks.add_to(m)
            fg_points.add_to(m)

    folium.LayerControl(collapsed=True).add_to(m)

    # --- Persist view in browser localStorage (no Streamlit rerun dependency) ---
    map_var = m.get_name()  # Folium's JS var name (e.g., "map_123abc")
    persist_js = f"""
    <script>
    (function() {{
      const KEY = 'ais_dashboard_map_view';
      function restore(map) {{
        try {{
          const s = localStorage.getItem(KEY);
          if (!s) return;
          const v = JSON.parse(s);
          if (!v || !isFinite(v.lat) || !isFinite(v.lng) || !isFinite(v.zoom)) return;
          map.setView([v.lat, v.lng], v.zoom);
        }} catch(e) {{}}
      }}
      function persist(map) {{
        try {{
          const c = map.getCenter();
          const z = map.getZoom();
          localStorage.setItem(KEY, JSON.stringify({{lat:c.lat, lng:c.lng, zoom:z}}));
        }} catch(e) {{}}
      }}
      const tryInit = () => {{
        const map = window['{map_var}'];
        if (!map) {{ return requestAnimationFrame(tryInit); }}
        restore(map);
        map.on('moveend zoomend', () => persist(map));
      }};
      tryInit();
    }})();
    </script>
    """
    m.get_root().html.add_child(folium.Element(persist_js))

    # Render (no snapping)
    st_html(m._repr_html_(), height=700, scrolling=False)


def tab_health():
    st.markdown("## 🧰 Database Health")

    SQL_TABLE_SIZES = """
    WITH rels AS (
      SELECT n.nspname AS schemaname,
             c.relname AS relname,
             format('%I.%I', n.nspname, c.relname) AS qualname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind IN ('r','m','p')
        AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    )
    SELECT
      qualname AS table,
      pg_total_relation_size(qualname)::bigint AS total_bytes,
      round(pg_total_relation_size(qualname)/1024.0/1024.0/1024.0, 3) AS size_gb
    FROM rels
    ORDER BY total_bytes DESC;
    """
    SQL_ROW_COUNTS = """
    SELECT schemaname || '.' || relname AS table, n_live_tup AS est_rows
    FROM pg_stat_user_tables
    ORDER BY n_live_tup DESC;
    """
    SQL_TSDB_FEATURES = """
    SELECT hypertable_schema || '.' || hypertable_name AS hypertable, compression_enabled
    FROM timescaledb_information.hypertables
    ORDER BY hypertable;
    """

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Table sizes (GB)")
        try:
            sizes = fetch_df(SQL_TABLE_SIZES)
            sizes["size_gb"] = sizes["size_gb"].astype(float).round(3)
            st.dataframe(sizes, width='stretch', height=360)
        except Exception as e:
            st.error(f"Could not fetch table sizes.\n\n{e}")

    with c2:
        st.subheader("Estimated row counts")
        try:
            rows = fetch_df(SQL_ROW_COUNTS)
            st.dataframe(rows, width='stretch', height=360)
        except Exception as e:
            st.error(f"Could not fetch row counts.\n\n{e}")

    st.markdown("---")
    st.subheader("TimescaleDB hypertables")
    try:
        feats = fetch_df(SQL_TSDB_FEATURES)
        if feats.empty:
            st.info("No hypertables detected (or Timescale not enabled).")
        else:
            st.dataframe(feats, width='stretch', height=240)
    except Exception:
        st.info("timescaledb_information not available.")

def tab_explorer():
    st.markdown("## 🔎 Explorer (SQL)")
    st.caption("Run read-only SQL. Tip: `LIMIT 100` to keep results snappy.")
    default_sql = "SELECT * FROM public.ais_fix ORDER BY ts DESC LIMIT 100;"
    sql = st.text_area("SQL", value=default_sql, height=180)
    run = st.button("Run query", type="primary")
    if run:
        try:
            df = fetch_df(sql)
            if df.empty:
                st.info("No rows.")
            else:
                st.dataframe(df, width='stretch', height=500)
        except Exception as e:
            st.error(f"Query failed:\n\n{e}")

# -----------------------------
# NEW ANALYTICS TABS
# -----------------------------
def tab_flows_and_lifts():
    st.markdown("## 📈 Flows & Lifts (CAGGs + EWMs)")

    # Choose port area for lifts (use core areas ideally)
    ports = fetch_df(SQL_AREA_PORTS)
    if ports.empty:
        st.info("No port areas found.")
        return
    p_choice = st.selectbox("Port area (for lifts)", ports["area_id"].tolist(), index=0)
    start = st.date_input("Start date", value=pd.Timestamp.utcnow().date() - pd.Timedelta(days=30))
    end   = st.date_input("End date", value=pd.Timestamp.utcnow().date())

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Port lifts — EWM")
        try:
            df = fetch_df(SQL_CA_PORT_LIFTS_EWM, {"area_id": p_choice, "start": start, "end": end})
            if df.empty:
                st.info("No EWM data for selected window.")
            else:
                df["day"] = pd.to_datetime(df["day"], utc=True)
                st.line_chart(df.set_index("day")[["arrive_dwt_ewm","depart_dwt_ewm"]], height=280)
        except Exception as e:
            st.error(f"Lifts EWM error: {e}")

    with c2:
        st.subheader("Port lifts — Daily raw (CAGG)")
        try:
            df = fetch_df(SQL_CA_PORT_LIFTS_DAILY, {"area_id": p_choice, "start": start, "end": end})
            if df.empty:
                st.info("No daily lifts for selected window.")
            else:
                df["day"] = pd.to_datetime(df["day"], utc=True)
                st.line_chart(df.set_index("day")[["arrive_dwt","depart_dwt"]], height=280)
        except Exception as e:
            st.error(f"Lifts daily error: {e}")

    st.markdown("---")
    lanes = fetch_df(SQL_LANES)
    if lanes.empty:
        st.info("No lanes/chokepoints found.")
        return
    l_choice = st.selectbox("Lane / chokepoint (for flows)", lanes["lane_id"].tolist(), index=0)

    st.subheader("Lane flows — EWM")
    try:
        df = fetch_df(SQL_CA_LANE_EWM, {"lane_id": l_choice, "start": start, "end": end})
        if df.empty:
            st.info("No lane EWM for selected window.")
        else:
            df["day"] = pd.to_datetime(df["day"], utc=True)
            st.line_chart(df.set_index("day")[["transit_cnt_ewm","laden_dwt_sum_ewm"]], height=280)
    except Exception as e:
        st.error(f"Lane flow EWM error: {e}")

def tab_queues_and_transit():
    st.markdown("## ⛴️ Queues & Transit Times")

    # Anchorage queues (approach areas)
    approaches = fetch_df("""
        SELECT area_id, name FROM public.area
        WHERE kind='port' AND subtype='approach'
        ORDER BY name, area_id;
    """)
    if approaches.empty:
        st.info("No approach areas found.")
        return
    a_choice = st.selectbox("Approach area (queues)", approaches["area_id"].tolist(), index=0)
    start = st.date_input("Start date", value=pd.Timestamp.utcnow().date() - pd.Timedelta(days=30), key="qs_start")
    end   = st.date_input("End date", value=pd.Timestamp.utcnow().date(), key="qs_end")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Anchorage queue — EWM")
        try:
            df = fetch_df(SQL_CA_ANCHOR_EWM, {"area_id": a_choice, "start": start, "end": end})
            if df.empty:
                st.info("No queue EWM in window.")
            else:
                df["day"] = pd.to_datetime(df["day"], utc=True)
                st.line_chart(df.set_index("day")[["queue_cnt_ewm"]], height=280)
        except Exception as e:
            st.error(f"Queue EWM error: {e}")

    with c2:
        st.subheader("Anchorage occupancy — Daily MV")
        try:
            df = fetch_df(SQL_MV_ANCHOR, {"area_id": a_choice, "start": start, "end": end})
            if df.empty:
                st.info("No occupancy MV in window.")
            else:
                df["day"] = pd.to_datetime(df["day"], utc=True)
                st.line_chart(df.set_index("day")[["avg_cnt","p50_cnt"]], height=280)
        except Exception as e:
            st.error(f"Occupancy MV error: {e}")

    st.markdown("---")
    lanes = fetch_df(SQL_LANES)
    if lanes.empty:
        st.info("No lanes/chokepoints found.")
        return
    l_choice = st.selectbox("Lane / chokepoint (transit time)", lanes["lane_id"].tolist(), index=0)

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Transit time — EWM (p50)")
        try:
            df = fetch_df(SQL_CA_TT_EWM, {"lane_id": l_choice, "start": start, "end": end})
            if df.empty:
                st.info("No TT EWM in window.")
            else:
                df["day"] = pd.to_datetime(df["day"], utc=True)
                st.line_chart(df.set_index("day")[["transit_time_p50_ewm"]], height=280)
        except Exception as e:
            st.error(f"Transit EWM error: {e}")

    with c4:
        st.subheader("Transit time — Daily MV (p50/p90/mean)")
        try:
            df = fetch_df(SQL_MV_LANE_TT, {"lane_id": l_choice, "start": start, "end": end})
            if df.empty:
                st.info("No transit MV in window.")
            else:
                df["day"] = pd.to_datetime(df["day"], utc=True)
                st.line_chart(df.set_index("day")[["p50_hours","p90_hours","mean_hours"]], height=280)
        except Exception as e:
            st.error(f"Transit MV error: {e}")

def tab_routing_and_mix():
    st.markdown("## 🔀 Routing & Class Mix")

    lanes = fetch_df(SQL_LANES)
    if lanes.empty:
        st.info("No lanes available.")
        return

    # Pick two lanes to compare routing share
    lane_ids = lanes["lane_id"].tolist()
    left_lane  = st.selectbox("Lane A (e.g., Cape)", lane_ids, index=0)
    right_lane = st.selectbox("Lane B (e.g., Mid-Atlantic)", lane_ids, index=min(1, len(lane_ids)-1))

    start = st.date_input("Start date", value=pd.Timestamp.utcnow().date() - pd.Timedelta(days=60), key="rm_start")
    end   = st.date_input("End date", value=pd.Timestamp.utcnow().date(), key="rm_end")

    # Fetch both EWMs
    def lane_flow(lid):
        df = fetch_df(SQL_CA_LANE_EWM, {"lane_id": lid, "start": start, "end": end})
        if not df.empty:
            df["day"] = pd.to_datetime(df["day"], utc=True)
            df = df.set_index("day")[["laden_dwt_sum_ewm"]].rename(columns={"laden_dwt_sum_ewm": lid})
        return df

    a = lane_flow(left_lane)
    b = lane_flow(right_lane)

    if a is None or a.empty or b is None or b.empty:
        st.info("Insufficient lane EWM data for routing share.")
    else:
        merged = a.join(b, how="outer").sort_index()
        merged["routing_share_A"] = merged[left_lane] / (merged[left_lane] + merged[right_lane])
        st.subheader("Routing share (A / (A + B))")
        st.line_chart(merged[["routing_share_A"]], height=280)

    st.markdown("---")
    # Class mix on a selected lane
    lane_mix = st.selectbox("Lane for class mix", lane_ids, index=0, key="lane_mix")
    try:
        mix = fetch_df(SQL_CA_CLASS_MIX, {"lane_id": lane_mix, "start": start, "end": end})
        if mix.empty:
            st.info("No class mix data.")
        else:
            mix["day"] = pd.to_datetime(mix["day"], utc=True)
            st.subheader("Class mix — EWMs")
            st.line_chart(mix.set_index("day")[["vlcc_share_ewm","suezmax_share_ewm","aframax_share_ewm"]], height=280)
    except Exception as e:
        st.error(f"Class mix error: {e}")

    st.markdown("---")
    # Ballast returns (lead signal)
    lane_ballast = st.selectbox("Lane for ballast return EWM", lane_ids, index=0, key="lane_ballast")
    try:
        bal = fetch_df(SQL_CA_BALLAST_EWM, {"lane_id": lane_ballast, "start": start, "end": end})
        if bal.empty:
            st.info("No ballast EWM data.")
        else:
            bal["day"] = pd.to_datetime(bal["day"], utc=True)
            st.subheader("Ballast return — EWM")
            st.line_chart(bal.set_index("day")[["ballast_dwt_ewm"]], height=280)
    except Exception as e:
        st.error(f"Ballast EWM error: {e}")

def tab_features():
    st.markdown("## 🧪 Final ML Features")

    start = st.date_input("Start date", value=pd.Timestamp.utcnow().date() - pd.Timedelta(days=90), key="fx_start")
    end   = st.date_input("End date", value=pd.Timestamp.utcnow().date(), key="fx_end")

    try:
        df = fetch_df(SQL_ML_FEATURES, {"start": start, "end": end})
        if df.empty:
            st.info("No ml_features_daily rows yet. Build/populate the view first.")
            return
        df["day"] = pd.to_datetime(df["day"], utc=True)
        st.dataframe(df.tail(200), width='stretch', height=360)

        # Quick pick of a few columns if present
        pick_cols = [c for c in df.columns if c not in ("day",)]
        if pick_cols:
            st.subheader("Quick chart")
            sel = st.multiselect("Columns to plot", pick_cols[:10], default=pick_cols[:3])
            if sel:
                st.line_chart(df.set_index("day")[sel], height=280)
    except Exception as e:
        st.error(f"Load features failed: {e}")

# -----------------------------
# App layout
# -----------------------------
st.title("🌊 AIS Dashboard 🌊")

tabs = st.tabs([
    "Overview", "Ports & Occupancy", "Map",
    "Flows & Lifts", "Queues & Transit", "Routing & Mix",
    "Features", "Database Health", "Explorer"
])
hours, map_age = header_kpis("overview")

with tabs[0]: tab_overview(hours)
with tabs[1]: tab_ports()
with tabs[2]: tab_map(map_age)
with tabs[3]: tab_flows_and_lifts()
with tabs[4]: tab_queues_and_transit()
with tabs[5]: tab_routing_and_mix()
with tabs[6]: tab_features()
with tabs[7]: tab_health()
with tabs[8]: tab_explorer()

st.sidebar.markdown("---")
st.sidebar.write(f"DB: `{PG_DSN}`")
st.sidebar.caption("Engine uses pool_pre_ping=True to avoid stale connections.")
