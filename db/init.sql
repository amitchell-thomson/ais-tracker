-- Enable TimescaleDB (time-series features) if not already present.
CREATE EXTENSION IF NOT EXISTS timescaledb;  -- ensures hypertable APIs are available

-- Enable PostGIS (geospatial types & functions) if not already present.
CREATE EXTENSION IF NOT EXISTS postgis;      -- gives geometry types, ST_Intersects, etc.

-- Drop in dependency-safe order if you are rebuilding from scratch (optional in new DBs).
DROP TABLE IF EXISTS public.ais_event CASCADE;      -- events depend on fixes; drop first if present
DROP TABLE IF EXISTS public.ais_fix CASCADE;        -- raw AIS fixes
DROP TABLE IF EXISTS public.area_gate CASCADE;      -- gates depend on areas
DROP TABLE IF EXISTS public.area_taxonomy CASCADE;  -- taxonomy depends on areas
DROP TABLE IF EXISTS public.area CASCADE;           -- base geofence polygons

-- Master geofence table: all polygons live here (ports, anchorages, lanes, chokepoints, sts).
CREATE TABLE public.area (
  area_id   text PRIMARY KEY,                                                 -- stable ID you choose (e.g., 'NL-RTM-core')
  name      text NOT NULL,                                                    -- human-readable name
  country   text,                                                             -- ISO country code or 'INTL'
  kind      text NOT NULL CHECK (kind IN ('port','anchorage','lane','chokepoint','sts')), -- classification of area
  role      text,                                                             -- sub-role (e.g., 'harbor_limit','approach','lane_corridor','sts_hotspot')
  priority  smallint DEFAULT 1,                                               -- 1=high, 2=med, 3=low for polling decisions
  geom      geometry(MultiPolygon,4326) NOT NULL                              -- the polygon(s) in WGS84
);

-- Spatial index for fast point-in-polygon queries.
CREATE INDEX IF NOT EXISTS idx_area_geom ON public.area USING gist (geom);    -- GiST index accelerates ST_Intersects/ST_DWithin

-- Gate polygons: one thin polygon per lane end (west/east or north/south).
CREATE TABLE public.area_gate (
  gate_id   text PRIMARY KEY,                                                 -- stable ID for each gate (e.g., 'G-MIDATL-W')
  area_id   text NOT NULL REFERENCES public.area(area_id) ON DELETE CASCADE,  -- parent lane/chokepoint this gate belongs to
  lane_end  text NOT NULL CHECK (lane_end IN ('west','east','north','south')),-- which end of the lane this gate represents
  geom      geometry(MultiPolygon,4326) NOT NULL                              -- thin gate polygon acting as a tripwire
);

-- Spatial index for gate intersections.
CREATE INDEX IF NOT EXISTS idx_area_gate_geom ON public.area_gate USING gist (geom); -- accelerates ST_Intersects with gate

-- Optional taxonomy to guide laden/ballast rules (even if you don’t use draft).
CREATE TABLE public.area_taxonomy (
  area_id   text PRIMARY KEY REFERENCES public.area(area_id) ON DELETE CASCADE, -- area being tagged
  flow_role text NOT NULL CHECK (flow_role IN ('export','import','mixed')),     -- typical flow role for that area
  commodity text NOT NULL CHECK (commodity IN ('crude','products','mixed'))     -- commodity flavor
);

-- Raw AIS fixes (both terrestrial & SAT) stored as a time-series hypertable.
CREATE TABLE public.ais_fix (
  ts             timestamptz NOT NULL,                                        -- observation timestamp (hypertable time)
  src            text NOT NULL CHECK (src IN ('terrestrial','sat')),          -- message source
  vessel_uid     text,                                                         -- stable vessel ID (IMO or hashed identity) when available
  sat_track_uid  text,                                                         -- continuity ID for SAT-only tracks (stitched by kinematics)
  lat            double precision,                                             -- latitude (degrees)
  lon            double precision,                                             -- longitude (degrees)
  sog            real,                                                         -- speed over ground (kn)
  cog            real,                                                         -- course over ground (deg); nullable and unused by gate logic
  heading        smallint,     
  elapsed        integer,                                                      -- minutes since last AIS message (from feed)                                                -- true heading (deg); often missing for SAT
  destination    text,                                                         -- destination string (terrestrial only)
  flag           text,                                                         -- vessel flag (terrestrial only)
  length_m       real,                                                         -- hull length (m; terrestrial only)
  width_m        real,                                                         -- hull width (m; terrestrial only)
  dwt            integer,                                                      -- deadweight (t; terrestrial only)
  shipname       text,                                                         -- vessel name (terrestrial; SAT may show “[SAT-AIS]”)
  shiptype       smallint,                                                     -- AIS ship type code
  ship_id        text,                                                         -- ship id when available
  rot            real,                                                         -- rate of turn; optional
  geom           geometry(Point,4326),                                         -- point geometry derived from lat/lon
  -- Derived classification flags (set by trigger in functions.sql)
  area_id_core   text,                                                         -- port harbor polygon ID if inside
  in_core        boolean,                                                      -- true if inside a port harbor polygon
  area_id_approach text,                                                       -- anchorage/approach polygon ID if inside
  in_approach    boolean,                                                      -- true if inside anchorage/approach polygon
  lane_id        text,                                                         -- lane/chokepoint corridor ID if inside
  in_lane        boolean,                                                      -- true if inside a lane/chokepoint corridor
  gate_id        text,                                                         -- gate polygon ID if current fix intersects a gate
  gate_end       text                                                          -- one of ('west','east','north','south') if in gate
);

-- Turn the fixes table into a hypertable partitioned by time.
SELECT create_hypertable('public.ais_fix','ts',if_not_exists=>TRUE);           -- enables chunking and policies

-- Helpful indexes to speed common lookups.
CREATE INDEX IF NOT EXISTS idx_fix_vessel_ts ON public.ais_fix (vessel_uid, ts DESC); -- per-vessel time scans
CREATE INDEX IF NOT EXISTS idx_fix_src_ts    ON public.ais_fix (src, ts DESC);        -- per-source time scans
CREATE INDEX IF NOT EXISTS idx_fix_geom      ON public.ais_fix USING gist (geom);     -- spatial searches
CREATE INDEX IF NOT EXISTS idx_fix_lane_ts   ON public.ais_fix (lane_id, ts DESC);    -- lane membership over time

-- Event table capturing transitions (port/approach enter/exit and lane enter/exit).
CREATE TABLE public.ais_event (
  ts            timestamptz NOT NULL,                                         -- event time (from the fix that caused it)
  vessel_uid    text,                                                         -- terrestrial/stable vessel ID when known
  sat_track_uid text,                                                         -- SAT continuity ID when only SAT is known
  event         text NOT NULL CHECK (event IN ('approach_enter','approach_exit','port_enter','port_exit','lane_enter','lane_exit')), -- event types
  area_id       text NOT NULL,                                                -- area implicated (port/anchorage/lane)
  area_kind     text NOT NULL CHECK (area_kind IN ('port','anchorage','lane','chokepoint')), -- area kind at event time
  gate_end      text,                                                         -- for lane events, which end ('west','east','north','south')
  lat           double precision,                                             -- snapshot lat at event
  lon           double precision,                                             -- snapshot lon at event
  meta          jsonb                                                         -- optional extras (e.g., {'src':'sat'})
);

-- Make events a hypertable as well.
SELECT create_hypertable('public.ais_event','ts',if_not_exists=>TRUE);         -- we want policies on events too

-- Indexes to query events by area and by vessel quickly.
CREATE INDEX IF NOT EXISTS idx_evt_area_ts   ON public.ais_event (area_id, ts DESC);  -- per-area event timeline
CREATE INDEX IF NOT EXISTS idx_evt_vessel_ts ON public.ais_event (vessel_uid, ts DESC);-- per-vessel event timeline

-- Compression for fixes: compress after 3 days; segment by vessel for better ratio.
ALTER TABLE public.ais_fix SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'vessel_uid',
  timescaledb.compress_orderby   = 'ts DESC'
);  -- enables columnar compression on older chunks

-- Compression policy: compress ais_fix rows older than 3 days.
SELECT add_compression_policy('public.ais_fix', INTERVAL '3 days');            -- automatic compression policy

-- Retention policy: keep 90 days of raw fixes (aggregates will keep long history separately).
SELECT add_retention_policy('public.ais_fix', INTERVAL '90 days');             -- drop chunks older than 90 days

-- Compression for events (less frequent than fixes, but still large over time).
ALTER TABLE public.ais_event SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'area_id',
  timescaledb.compress_orderby   = 'ts DESC'
);  -- compress older event chunks clustered by area

-- Compress events after 7 days to save space.
SELECT add_compression_policy('public.ais_event', INTERVAL '7 days');          -- compress older event chunks

-- Keep 2 years of events for historical analysis.
SELECT add_retention_policy('public.ais_event', INTERVAL '2 years');           -- drop ancient event chunks
