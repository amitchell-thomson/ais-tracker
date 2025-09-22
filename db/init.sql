-- ============================================================================
-- db/init.sql  —  AIS schema (Areas, Gates, Fixes, Events) for TimescaleDB + PostGIS
-- PostgreSQL 15+, TimescaleDB (TSL for compression/retention), PostGIS present
-- ============================================================================

-- Extensions -----------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;   -- hypertables & policies
CREATE EXTENSION IF NOT EXISTS postgis;       -- geometry types & spatial ops

-- Drops (safe order) ----------------------------------------------------------
DROP TABLE IF EXISTS public.ais_event      CASCADE;  -- depends on ais_fix
DROP TABLE IF EXISTS public.ais_fix        CASCADE;
DROP TABLE IF EXISTS public.area_gate      CASCADE;  -- depends on area
DROP TABLE IF EXISTS public.area_taxonomy  CASCADE;  -- legacy (if ever existed)
DROP TABLE IF EXISTS public.area           CASCADE;

-- ============================================================================
-- Geofence tables
-- ============================================================================

-- Master geofence polygons: ports, approaches, corridors, chokepoints, STS.
CREATE TABLE public.area (
  area_id   text PRIMARY KEY,                                           -- stable text id (e.g., 'NL-RTM-core' or hashed)
  name      text NOT NULL,                                              -- human-readable label
  kind      text NOT NULL CHECK (kind IN ('port','lane','chokepoint','sts')), -- high-level class from data
  subtype   text NOT NULL CHECK (subtype IN ('core','approach','corridor')),  -- finer class
  "group"   text,                                                       -- logical cluster/family (nullable; many ports have none)
  notes     text,                                                       -- freeform context/explanation
  geom      geometry(Polygon,4326) NOT NULL,                            -- WGS84 polygon geometry
  flow_role text CHECK (flow_role IN ('export','import','mixed'))       -- optional: typical flow role, if you use it
);

-- Spatial + attribute indexes for area
CREATE INDEX IF NOT EXISTS idx_area_geom        ON public.area USING gist (geom);           -- spatial lookups
CREATE INDEX IF NOT EXISTS idx_area_kind_group  ON public.area (kind, subtype, "group");    -- filter by class/group fast

-- Gate polygons: thin polygons acting as directional checkpoints on corridors/chokepoints.
CREATE TABLE public.area_gate (
  gate_id   text PRIMARY KEY,                                                -- stable text id for gate
  area_id   text NOT NULL REFERENCES public.area(area_id) ON DELETE CASCADE, -- parent corridor/chokepoint
  name      text NOT NULL,                                                   -- gate name (e.g., 'Gibraltar — gate_east')
  kind      text NOT NULL CHECK (kind IN ('lane','chokepoint')),             -- mirror parent kind for convenience
  subtype   text NOT NULL CHECK (subtype IN ('gate_west','gate_east','gate_north','gate_south')), -- encoded side
  "group"   text,                                                            -- mirror of parent group (nullable)
  notes     text,                                                            -- context/explanation
  geom      geometry(Polygon,4326) NOT NULL                                  -- thin polygon (can switch to LineString later)
);

-- Spatial + parent/side indexes for gates
CREATE INDEX IF NOT EXISTS idx_area_gate_geom    ON public.area_gate USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_area_gate_parent  ON public.area_gate (area_id, subtype);

-- ============================================================================
-- AIS time-series (TimescaleDB)
-- ============================================================================

-- Raw AIS fixes (points). Hypertable on ts.
CREATE TABLE public.ais_fix (
  ts               timestamptz NOT NULL,                              -- observation timestamp (hypertable time)
  src              text NOT NULL CHECK (src IN ('terrestrial','sat')),-- data source
  vessel_uid       text,                                              -- stable vessel id (IMO/MMSI or hashed surrogate)
  sat_track_uid    text,                                              -- continuity id for SAT-only tracks
  lat              double precision,                                  -- latitude (deg)
  lon              double precision,                                  -- longitude (deg)
  sog              real,                                              -- speed over ground (knots)
  cog              real,                                              -- course over ground (deg)
  heading          smallint,                                          -- true heading (deg) when available
  elapsed          integer,                                           -- minutes since last message per feed
  destination      text,                                              -- freeform destination
  flag             text,                                              -- vessel flag
  length_m         real,                                              -- hull length (m)
  width_m          real,                                              -- beam (m)
  dwt              integer,                                           -- deadweight (t)
  shipname         text,                                              -- vessel name (may be "[SAT-AIS]" for SAT tracks)
  shiptype         smallint,                                          -- AIS ship type code
  ship_id          text,                                              -- provider ship id when available
  rot              real,                                              -- rate of turn (deg/min), optional
  geom             geometry(Point,4326),                              -- point geometry from lat/lon

  -- Derived memberships / flags (set by downstream process or triggers)
  area_id_core       text,                                           -- area.area_id for core port polygon if inside
  in_core            boolean,                                        -- is inside a core port polygon
  area_id_approach   text,                                           -- area.area_id for approach polygon if inside
  in_approach        boolean,                                        -- is inside an approach polygon
  lane_id            text,                                           -- area.area_id for corridor/chokepoint if inside
  in_lane            boolean,                                        -- is inside a corridor/chokepoint polygon
  gate_id            text,                                           -- area_gate.gate_id if intersecting a gate
  gate_end           text CHECK (gate_end IN ('west','east','north','south')) -- side label, if applicable
);

-- Turn fixes into hypertable
SELECT create_hypertable('public.ais_fix','ts', if_not_exists => TRUE);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_fix_vessel_ts ON public.ais_fix (vessel_uid, ts DESC);  -- fast per-vessel scans
CREATE INDEX IF NOT EXISTS idx_fix_src_ts    ON public.ais_fix (src, ts DESC);         -- per-source scans
CREATE INDEX IF NOT EXISTS idx_fix_geom      ON public.ais_fix USING gist (geom);      -- spatial proximity
CREATE INDEX IF NOT EXISTS idx_fix_lane_ts   ON public.ais_fix (lane_id, ts DESC);     -- corridor membership over time

-- Event stream: normalized transitions generated from fixes.
CREATE TABLE public.ais_event (
  ts            timestamptz NOT NULL,                                           -- event timestamp (from triggering fix)
  vessel_uid    text,                                                           -- stable vessel id (or surrogate)
  sat_track_uid text,                                                           -- SAT track continuity id
  event         text NOT NULL CHECK (event IN (                                 -- event verb
                    'approach_enter','approach_exit',
                    'port_enter','port_exit',
                    'lane_enter','lane_exit'
                  )),
  area_id       text NOT NULL,                                                  -- implicated area (port/approach/corridor)
  area_kind     text NOT NULL CHECK (area_kind IN ('port','approach','lane','chokepoint','sts')), -- class at event time
  gate_end      text CHECK (gate_end IN ('west','east','north','south')),       -- side for lane events, if known
  lat           double precision,                                               -- snapshot latitude at event
  lon           double precision,                                               -- snapshot longitude at event
  meta          jsonb                                                           -- optional details (e.g., {"src":"sat"})
);

-- Turn events into hypertable
SELECT create_hypertable('public.ais_event','ts', if_not_exists => TRUE);

-- Event indexes
CREATE INDEX IF NOT EXISTS idx_evt_area_ts   ON public.ais_event (area_id, ts DESC);   -- per-area timeline
CREATE INDEX IF NOT EXISTS idx_evt_vessel_ts ON public.ais_event (vessel_uid, ts DESC);-- per-vessel timeline

-- ============================================================================
-- TimescaleDB policies (require TSL / license=timescale)
-- Comment these out if you're running Apache-only.
-- ============================================================================

-- Compress fixes: segment by vessel, order by time desc; compress after 3 days.
ALTER TABLE public.ais_fix SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'vessel_uid',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('public.ais_fix', INTERVAL '3 days');

-- Retain only 90 days of raw fixes (aggregations elsewhere keep long history).
SELECT add_retention_policy('public.ais_fix', INTERVAL '90 days');

-- Compress events: segment by area, order by time desc; compress after 7 days.
ALTER TABLE public.ais_event SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'area_id',
  timescaledb.compress_orderby   = 'ts DESC'
);
SELECT add_compression_policy('public.ais_event', INTERVAL '7 days');

-- Keep two years of events.
SELECT add_retention_policy('public.ais_event', INTERVAL '2 years');
