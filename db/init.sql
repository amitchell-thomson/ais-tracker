-- ============================================================================
-- db/init.sql — AIS schema (Areas, Gates, Fixes, Events) for TimescaleDB + PostGIS
-- PostgreSQL 15+, TimescaleDB (TSL for compression/retention), PostGIS present
-- ============================================================================

-- Extensions -----------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;   -- hypertables & policies
CREATE EXTENSION IF NOT EXISTS postgis;       -- geometry types & spatial ops

-- Drops (safe order) ----------------------------------------------------------
DROP TABLE IF EXISTS public.ais_event      CASCADE;  -- depends on ais_fix
DROP TABLE IF EXISTS public.ais_fix        CASCADE;
DROP TABLE IF EXISTS public.area_gate      CASCADE;  -- depends on area
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
  "group"   text,                                                       -- logical cluster/family (nullable)
  notes     text,                                                       -- freeform context/explanation
  geom      geometry(Polygon,4326) NOT NULL,                            -- WGS84 polygon geometry
  flow_role text CHECK (flow_role IN ('export','import','mixed'))       -- optional: typical flow role
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
  lat              double precision CHECK (lat BETWEEN -90 AND 90),   -- latitude (deg)
  lon              double precision CHECK (lon BETWEEN -180 AND 180), -- longitude (deg)
  sog              real,                                              -- speed over ground (knots)
  cog              real,                                              -- course over ground (deg 0..360, can be NULL)
  heading          smallint,                                          -- true heading (deg 0..360, can be NULL)
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

  -- If not provided by the collector, geom can be derived in BEFORE trigger.
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
CREATE INDEX IF NOT EXISTS idx_fix_lane_ts   ON public.ais_fix (lane_id, ts DESC);     -- corridor membership over time
CREATE INDEX IF NOT EXISTS idx_fix_geom      ON public.ais_fix USING gist (geom);      -- spatial proximity

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

-- ============================================================================
-- Helper: ensure geom exists if lat/lon present (runs before labelling/dedup)
-- ============================================================================
CREATE OR REPLACE FUNCTION f_ensure_geom() RETURNS trigger AS $$
BEGIN
  IF NEW.geom IS NULL AND NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_b0_ensure_geom ON public.ais_fix;
CREATE TRIGGER trg_b0_ensure_geom
BEFORE INSERT ON public.ais_fix
FOR EACH ROW
EXECUTE FUNCTION f_ensure_geom();

-- ============================================================================
-- BEFORE: label fix membership (uses ST_Covers to include boundaries)
-- ============================================================================
CREATE OR REPLACE FUNCTION f_label_fix_membership() RETURNS trigger AS $$
BEGIN
  -- Core port membership
  SELECT a.area_id
    INTO NEW.area_id_core
  FROM public.area a
  WHERE a.kind = 'port' AND a.subtype = 'core'
    AND ST_Covers(a.geom, NEW.geom)
  ORDER BY ST_Area(a.geom) ASC
  LIMIT 1;
  NEW.in_core := NEW.area_id_core IS NOT NULL;

  -- Approach membership
  SELECT a.area_id
    INTO NEW.area_id_approach
  FROM public.area a
  WHERE a.kind = 'port' AND a.subtype = 'approach'
    AND ST_Covers(a.geom, NEW.geom)
  ORDER BY ST_Area(a.geom) ASC
  LIMIT 1;
  NEW.in_approach := NEW.area_id_approach IS NOT NULL;

  -- Lane/chokepoint membership
  SELECT a.area_id
    INTO NEW.lane_id
  FROM public.area a
  WHERE a.subtype = 'corridor'
    AND ST_Covers(a.geom, NEW.geom)
  ORDER BY ST_Area(a.geom) ASC
  LIMIT 1;
  NEW.in_lane := NEW.lane_id IS NOT NULL;

  -- Gate hit: map 'gate_west' -> 'west', etc.
  SELECT
    ag.gate_id,
    CASE ag.subtype
      WHEN 'gate_west'  THEN 'west'
      WHEN 'gate_east'  THEN 'east'
      WHEN 'gate_north' THEN 'north'
      WHEN 'gate_south' THEN 'south'
      ELSE NULL
    END
  INTO NEW.gate_id, NEW.gate_end
  FROM public.area_gate ag
  WHERE ST_Covers(ag.geom, NEW.geom)
  ORDER BY ST_Area(ag.geom) ASC
  LIMIT 1;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_b1_label_fix_membership ON public.ais_fix;
CREATE TRIGGER trg_b1_label_fix_membership
BEFORE INSERT ON public.ais_fix
FOR EACH ROW
EXECUTE FUNCTION f_label_fix_membership();

-- ============================================================================
-- BEFORE: de-duplication trigger — skip insert when nothing useful changed
-- ============================================================================
-- Angular difference helper (0..180)
CREATE OR REPLACE FUNCTION f_ang_diff(a1 DOUBLE PRECISION, a2 DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
IMMUTABLE
LANGUAGE sql AS $$
  SELECT CASE
           WHEN a1 IS NULL OR a2 IS NULL THEN NULL
           ELSE LEAST(ABS(a1 - a2), 360 - ABS(a1 - a2))
         END;
$$;

CREATE OR REPLACE FUNCTION f_dedup_fix() RETURNS trigger AS $$
DECLARE
  prev RECORD;
  pos_same BOOLEAN;
  sog_same BOOLEAN;
  cog_same BOOLEAN;
  hdg_same BOOLEAN;
  areas_same BOOLEAN;
  gates_same BOOLEAN;

  -- Tolerances
  m_pos_meters CONSTANT DOUBLE PRECISION := 50;   -- ≤ 5 m considered same position
  m_sog_kn     CONSTANT DOUBLE PRECISION := 0.5;   -- ≤ 0.1 kn same speed
  m_ang_deg    CONSTANT DOUBLE PRECISION := 2.0;   -- ≤ 1° same angle
BEGIN
  -- If we cannot key by vessel, do not dedup (SAT ghost without UID)
  IF NEW.vessel_uid IS NULL THEN
    RETURN NEW;
  END IF;

  -- Fetch the most recent prior fix for this vessel at or before NEW.ts
  SELECT *
  INTO prev
  FROM public.ais_fix
  WHERE vessel_uid = NEW.vessel_uid
    AND ts <= NEW.ts
  ORDER BY ts DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN NEW;  -- no prior state -> allow insert
  END IF;

  -- Compare with tolerances
  pos_same := (NEW.geom IS NOT NULL AND prev.geom IS NOT NULL)
              AND ST_DWithin(prev.geom::geography, NEW.geom::geography, m_pos_meters);

  sog_same := (NEW.sog IS NOT DISTINCT FROM prev.sog)
              OR (NEW.sog IS NOT NULL AND prev.sog IS NOT NULL AND ABS(NEW.sog - prev.sog) <= m_sog_kn);

  cog_same := (NEW.cog IS NOT DISTINCT FROM prev.cog)
              OR (NEW.cog IS NOT NULL AND prev.cog IS NOT NULL AND f_ang_diff(NEW.cog, prev.cog) <= m_ang_deg);

  hdg_same := (NEW.heading IS NOT DISTINCT FROM prev.heading)
              OR (NEW.heading IS NOT NULL AND prev.heading IS NOT NULL AND f_ang_diff(NEW.heading, prev.heading) <= m_ang_deg);

  areas_same := (NEW.area_id_core     IS NOT DISTINCT FROM prev.area_id_core)
             AND (NEW.area_id_approach IS NOT DISTINCT FROM prev.area_id_approach)
             AND (NEW.lane_id          IS NOT DISTINCT FROM prev.lane_id)
             AND (NEW.in_core          IS NOT DISTINCT FROM prev.in_core)
             AND (NEW.in_approach      IS NOT DISTINCT FROM prev.in_approach)
             AND (NEW.in_lane          IS NOT DISTINCT FROM prev.in_lane);

  gates_same := (NEW.gate_id IS NOT DISTINCT FROM prev.gate_id)
             AND (NEW.gate_end IS NOT DISTINCT FROM prev.gate_end);

  -- If position AND kinematics AND memberships are unchanged -> skip insert
  IF pos_same AND sog_same AND cog_same AND hdg_same AND areas_same AND gates_same THEN
    RETURN NULL; -- drop row
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_b2_dedup_fix ON public.ais_fix;
CREATE TRIGGER trg_b2_dedup_fix
BEFORE INSERT ON public.ais_fix
FOR EACH ROW
EXECUTE FUNCTION f_dedup_fix();

-- ============================================================================
-- Vessel state + cargo state: eventization on transitions (guard for NULL UID)
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.vessel_state (
  vessel_uid    text PRIMARY KEY,
  sat_track_uid text,
  core_area_id  text,
  approach_area_id text,
  lane_id       text,
  gate_end      text,        -- last seen side
  updated_ts    timestamptz  -- last processed fix time
);

CREATE TABLE IF NOT EXISTS public.vessel_cargo_state (
  vessel_uid  text PRIMARY KEY,
  laden       boolean,           -- current best guess
  confidence  text,              -- 'derived_port_exit' | 'derived_approach_enter' | 'unknown'
  updated_ts  timestamptz
);

CREATE OR REPLACE FUNCTION f_emit_events() RETURNS trigger AS $$
DECLARE
  s public.vessel_state;
  cs public.vessel_cargo_state;
  port_role text; -- 'export'|'import'|'mixed'|NULL
  cur_laden boolean;
  cur_conf  text;
BEGIN
  IF NEW.vessel_uid IS NULL THEN
    RETURN NEW;
  END IF;

  -- Load last vessel state and cargo state
  SELECT * INTO s  FROM public.vessel_state        WHERE vessel_uid = NEW.vessel_uid;
  SELECT * INTO cs FROM public.vessel_cargo_state  WHERE vessel_uid = NEW.vessel_uid;

  -- Helper: fetch port flow_role by area_id
  -- (used only when we emit port_* events below)
  -- NOTE: We query when needed, not up-front, to avoid extra lookups.
  -- ---------------------------------------------------------------

  -- =========================
  -- Core port transitions
  -- =========================
  IF (s.core_area_id IS DISTINCT FROM NEW.area_id_core) THEN
    IF NEW.area_id_core IS NOT NULL THEN
      -- PORT ENTER
      SELECT flow_role INTO port_role FROM public.area WHERE area_id = NEW.area_id_core;

      INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
      VALUES (
        NEW.ts, NEW.vessel_uid, NEW.sat_track_uid,
        'port_enter', NEW.area_id_core, 'port', NULL, NEW.lat, NEW.lon,
        jsonb_build_object(
          'src', NEW.src,
          'dwt', NEW.dwt,
          -- optional arrival hint (doesn't change state, just informational)
          'arrival_likely_laden', CASE WHEN port_role='import' THEN true
                                       WHEN port_role='export' THEN false
                                       ELSE NULL END
        )
      );

      -- Optionally initialize cargo state on enter if unknown:
      IF cs.vessel_uid IS NULL THEN
        cur_laden := CASE WHEN port_role='import' THEN true
                          WHEN port_role='export' THEN false
                          ELSE NULL END;
        cur_conf  := CASE WHEN port_role IN ('import','export') THEN 'derived_approach_enter' ELSE 'unknown' END;

        IF cur_laden IS NOT NULL THEN
          INSERT INTO public.vessel_cargo_state(vessel_uid, laden, confidence, updated_ts)
          VALUES (NEW.vessel_uid, cur_laden, cur_conf, NEW.ts)
          ON CONFLICT (vessel_uid) DO UPDATE
          SET laden = EXCLUDED.laden, confidence = EXCLUDED.confidence, updated_ts = EXCLUDED.updated_ts;
        END IF;
      END IF;

    ELSIF s.core_area_id IS NOT NULL THEN
      -- PORT EXIT
      SELECT flow_role INTO port_role FROM public.area WHERE area_id = s.core_area_id;

      -- Determine cargo state on exit: export -> laden; import -> unladen; mixed -> unknown
      cur_laden := CASE WHEN port_role='export' THEN true
                        WHEN port_role='import' THEN false
                        ELSE NULL END;
      cur_conf  := CASE WHEN port_role IN ('export','import') THEN 'derived_port_exit' ELSE 'unknown' END;

      -- Upsert cargo state if known
      IF cur_laden IS NOT NULL THEN
        INSERT INTO public.vessel_cargo_state(vessel_uid, laden, confidence, updated_ts)
        VALUES (NEW.vessel_uid, cur_laden, cur_conf, NEW.ts)
        ON CONFLICT (vessel_uid) DO UPDATE
        SET laden = EXCLUDED.laden,
            confidence = EXCLUDED.confidence,
            updated_ts = EXCLUDED.updated_ts;
      END IF;

      -- Emit event with cargo state + dwt stamped in meta
      INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
      VALUES (
        NEW.ts, NEW.vessel_uid, NEW.sat_track_uid,
        'port_exit', s.core_area_id, 'port', NULL, NEW.lat, NEW.lon,
        jsonb_build_object(
          'src', NEW.src,
          'dwt', NEW.dwt,
          'laden', cur_laden,
          'cargo_confidence', cur_conf,
          'port_flow_role', port_role
        )
      );
    END IF;
  END IF;

  -- =========================
  -- Approach transitions
  -- =========================
  IF (s.approach_area_id IS DISTINCT FROM NEW.area_id_approach) THEN
    -- Inherit known cargo state for stamping
    SELECT * INTO cs FROM public.vessel_cargo_state WHERE vessel_uid = NEW.vessel_uid;

    IF NEW.area_id_approach IS NOT NULL THEN
      INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
      VALUES (
        NEW.ts, NEW.vessel_uid, NEW.sat_track_uid,
        'approach_enter', NEW.area_id_approach, 'approach', NULL, NEW.lat, NEW.lon,
        jsonb_build_object('src', NEW.src, 'dwt', NEW.dwt, 'laden', cs.laden, 'cargo_confidence', cs.confidence)
      );
    ELSIF s.approach_area_id IS NOT NULL THEN
      INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
      VALUES (
        NEW.ts, NEW.vessel_uid, NEW.sat_track_uid,
        'approach_exit', s.approach_area_id, 'approach', NULL, NEW.lat, NEW.lon,
        jsonb_build_object('src', NEW.src, 'dwt', NEW.dwt, 'laden', cs.laden, 'cargo_confidence', cs.confidence)
      );
    END IF;
  END IF;

  -- =========================
  -- Lane/corridor transitions
  -- =========================
  IF (s.lane_id IS DISTINCT FROM NEW.lane_id) THEN
    -- Inherit cargo state when stamping lane events
    SELECT * INTO cs FROM public.vessel_cargo_state WHERE vessel_uid = NEW.vessel_uid;

    IF NEW.lane_id IS NOT NULL THEN
      INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
      VALUES (
        NEW.ts, NEW.vessel_uid, NEW.sat_track_uid,
        'lane_enter', NEW.lane_id, 'lane', NEW.gate_end, NEW.lat, NEW.lon,
        jsonb_build_object('src', NEW.src, 'dwt', NEW.dwt, 'laden', cs.laden, 'cargo_confidence', cs.confidence)
      );
    ELSIF s.lane_id IS NOT NULL THEN
      INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
      VALUES (
        NEW.ts, NEW.vessel_uid, NEW.sat_track_uid,
        'lane_exit', s.lane_id, 'lane', NEW.gate_end, NEW.lat, NEW.lon,
        jsonb_build_object('src', NEW.src, 'dwt', NEW.dwt, 'laden', cs.laden, 'cargo_confidence', cs.confidence)
      );
    END IF;
  END IF;

  -- =========================
  -- Upsert new positional state (unchanged from your version)
  -- =========================
  INSERT INTO public.vessel_state(vessel_uid, sat_track_uid, core_area_id, approach_area_id, lane_id, gate_end, updated_ts)
  VALUES (NEW.vessel_uid, NEW.sat_track_uid, NEW.area_id_core, NEW.area_id_approach, NEW.lane_id, NEW.gate_end, NEW.ts)
  ON CONFLICT (vessel_uid) DO UPDATE
  SET sat_track_uid    = EXCLUDED.sat_track_uid,
      core_area_id     = EXCLUDED.core_area_id,
      approach_area_id = EXCLUDED.approach_area_id,
      lane_id          = EXCLUDED.lane_id,
      gate_end         = EXCLUDED.gate_end,
      updated_ts       = EXCLUDED.updated_ts;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Vessel dwell sessions (derived from events)
-- Open on *_enter, close on *_exit; tolerant to duplicates/out-of-order
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.vessel_dwell_session (
  vessel_uid   text        NOT NULL,
  area_id      text        NOT NULL,                 -- matches ais_event.area_id
  area_kind    text        NOT NULL,                 -- 'port' | 'approach' | 'lane' | 'chokepoint' | 'sts'
  start_ts     timestamptz NOT NULL,                 -- first ENTER timestamp
  end_ts       timestamptz,                          -- EXIT timestamp (NULL while open)
  duration_s   bigint,                               -- seconds; filled on close
  is_open      boolean     NOT NULL DEFAULT true,    -- open until EXIT
  samples      integer     NOT NULL DEFAULT 0,       -- event count contributing to this session
  first_lat    double precision,                     -- lat/lon at ENTER if provided
  first_lon    double precision,
  last_lat     double precision,                     -- last lat/lon seen from events
  last_lon     double precision,
  source       text        NOT NULL DEFAULT 'event', -- 'event' | 'repair' (if later reconciled)
  PRIMARY KEY (vessel_uid, area_id, start_ts)
);

-- Fast lookups + guard for single open session per vessel+area
CREATE INDEX IF NOT EXISTS ix_dwell_area_time   ON public.vessel_dwell_session (area_id, COALESCE(end_ts,start_ts) DESC);
CREATE INDEX IF NOT EXISTS ix_dwell_vessel_time ON public.vessel_dwell_session (vessel_uid, COALESCE(end_ts,start_ts) DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dwell_open
  ON public.vessel_dwell_session (vessel_uid, area_id)
  WHERE is_open = true;

CREATE OR REPLACE FUNCTION public.f_dwell_from_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  s public.vessel_dwell_session;
  is_enter boolean := (NEW.event IN ('port_enter','approach_enter','lane_enter'));
  is_exit  boolean := (NEW.event IN ('port_exit','approach_exit','lane_exit'));
BEGIN
  -- Ignore events without a vessel key
  IF NEW.vessel_uid IS NULL THEN
    RETURN NEW;
  END IF;

  IF is_enter THEN
    -- If open session exists, treat as duplicate/out-of-order ENTER
    SELECT * INTO s
    FROM public.vessel_dwell_session
    WHERE vessel_uid=NEW.vessel_uid AND area_id=NEW.area_id AND is_open
    ORDER BY start_ts DESC
    LIMIT 1;

    IF FOUND THEN
      -- Pull start earlier if this ENTER predates the current start
      IF NEW.ts < s.start_ts THEN
        UPDATE public.vessel_dwell_session
        SET start_ts = NEW.ts,
            first_lat = COALESCE(first_lat, NEW.lat),
            first_lon = COALESCE(first_lon, NEW.lon),
            samples   = samples + 1
        WHERE vessel_uid = s.vessel_uid AND area_id = s.area_id AND start_ts = s.start_ts;
      ELSE
        UPDATE public.vessel_dwell_session
        SET samples = samples + 1,
            last_lat = COALESCE(NEW.lat, last_lat),
            last_lon = COALESCE(NEW.lon, last_lon)
        WHERE vessel_uid = s.vessel_uid AND area_id = s.area_id AND start_ts = s.start_ts;
      END IF;
    ELSE
      -- Open new session
      INSERT INTO public.vessel_dwell_session
        (vessel_uid, area_id, area_kind, start_ts, end_ts, duration_s, is_open,
         samples, first_lat, first_lon, last_lat, last_lon, source)
      VALUES
        (NEW.vessel_uid, NEW.area_id, NEW.area_kind, NEW.ts, NULL, NULL, true,
         1, NEW.lat, NEW.lon, NEW.lat, NEW.lon, 'event');
    END IF;

    RETURN NEW;
  END IF;

  IF is_exit THEN
    -- Close the newest open session
    SELECT * INTO s
    FROM public.vessel_dwell_session
    WHERE vessel_uid=NEW.vessel_uid AND area_id=NEW.area_id AND is_open
    ORDER BY start_ts DESC
    LIMIT 1;

    IF FOUND THEN
      UPDATE public.vessel_dwell_session
      SET end_ts     = GREATEST(NEW.ts, start_ts), -- guard clock skew
          duration_s = GREATEST(1, EXTRACT(EPOCH FROM (GREATEST(NEW.ts, start_ts) - start_ts))::bigint),
          is_open    = false,
          samples    = samples + 1,
          last_lat   = COALESCE(NEW.lat, last_lat),
          last_lon   = COALESCE(NEW.lon, last_lon)
      WHERE vessel_uid = s.vessel_uid AND area_id = s.area_id AND start_ts = s.start_ts;
    ELSE
      -- EXIT without known ENTER: create 1s stub to keep stats consistent
      INSERT INTO public.vessel_dwell_session
        (vessel_uid, area_id, area_kind, start_ts, end_ts, duration_s, is_open,
         samples, first_lat, first_lon, last_lat, last_lon, source)
      VALUES
        (NEW.vessel_uid, NEW.area_id, NEW.area_kind, NEW.ts, NEW.ts, 1, false,
         1, NEW.lat, NEW.lon, NEW.lat, NEW.lon, 'event');
    END IF;

    RETURN NEW;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dwell_from_event ON public.ais_event;
CREATE TRIGGER trg_dwell_from_event
AFTER INSERT ON public.ais_event
FOR EACH ROW
EXECUTE FUNCTION public.f_dwell_from_event();
