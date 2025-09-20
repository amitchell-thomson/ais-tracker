-- Build a point geometry from lat/lon if needed.
CREATE OR REPLACE FUNCTION public.ensure_geom(lat double precision, lon double precision)
RETURNS geometry
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN lat IS NULL OR lon IS NULL THEN NULL                 -- if missing coords, return NULL
    ELSE ST_SetSRID(ST_MakePoint(lon, lat), 4326)             -- build WGS84 point (lon first!)
  END;
$$;

-- Return first matching core/approach/lane memberships for a given point.
CREATE OR REPLACE FUNCTION public.classify_membership(g geometry)
RETURNS TABLE(
  area_id_core text,    -- harbor polygon ID if inside
  in_core boolean,      -- true if inside harbor polygon
  area_id_approach text,-- anchorage/approach polygon ID if inside
  in_approach boolean,  -- true if inside anchorage polygon
  lane_id text,         -- lane/chokepoint corridor ID if inside
  in_lane boolean       -- true if inside lane polygon
)
LANGUAGE sql
STABLE
AS $$
  WITH core AS (
    SELECT a.area_id
    FROM public.area a
    WHERE a.kind='port' AND COALESCE(a.role,'')='harbor_limit' AND ST_Intersects(a.geom, g)
    LIMIT 1                                                 -- first core port polygon hit (you can refine order by priority)
  ),
  anch AS (
    SELECT a.area_id
    FROM public.area a
    WHERE a.kind='anchorage' AND ST_Intersects(a.geom, g)
    LIMIT 1                                                 -- first anchorage polygon hit
  ),
  lane AS (
    SELECT a.area_id
    FROM public.area a
    WHERE a.kind IN ('lane','chokepoint') AND ST_Intersects(a.geom, g)
    LIMIT 1                                                 -- first lane/chokepoint corridor hit
  )
  SELECT
    (SELECT area_id FROM core)             AS area_id_core,     -- harbor ID or NULL
    (SELECT area_id IS NOT NULL FROM core) AS in_core,          -- harbor membership flag
    (SELECT area_id FROM anch)             AS area_id_approach, -- anchorage ID or NULL
    (SELECT area_id IS NOT NULL FROM anch) AS in_approach,      -- anchorage membership flag
    (SELECT area_id FROM lane)             AS lane_id,          -- lane/corridor ID or NULL
    (SELECT area_id IS NOT NULL FROM lane) AS in_lane;          -- lane membership flag
$$;

-- Identify which gate polygon (if any) a point touches, and which end it is.
CREATE OR REPLACE FUNCTION public.classify_gate(g geometry)
RETURNS TABLE(
  gate_id text,      -- gate polygon ID if inside
  lane_end text,     -- which end ('west','east','north','south') that gate represents
  area_id text       -- the parent lane/chokepoint area ID this gate belongs to
)
LANGUAGE sql
STABLE
AS $$
  SELECT ag.gate_id, ag.lane_end, ag.area_id
  FROM public.area_gate ag
  WHERE ST_Intersects(ag.geom, g)
  LIMIT 1;                                               -- first gate hit (at most one expected if gates are thin/slab-like)
$$;

-- BEFORE INSERT trigger: ensure geometry and set classification flags & IDs on the new fix row.
CREATE OR REPLACE FUNCTION public.trg_before_classify_fix()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Build geometry if caller didn't provide it.
  IF NEW.geom IS NULL THEN
    NEW.geom := public.ensure_geom(NEW.lat, NEW.lon);    -- set NEW.geom from lat/lon
  END IF;

  -- If we still don't have a geometry (bad data), just let the row insert (no classification).
  IF NEW.geom IS NULL THEN
    RETURN NEW;                                          -- nothing else we can classify
  END IF;

  -- Lookup port/anchorage/lane memberships at this point.
  SELECT area_id_core, in_core, area_id_approach, in_approach, lane_id, in_lane
    INTO NEW.area_id_core, NEW.in_core, NEW.area_id_approach, NEW.in_approach, NEW.lane_id, NEW.in_lane
  FROM public.classify_membership(NEW.geom);             -- fills the 6 derived columns

  -- Lookup gate (if any) touched at this point (independent of lane flag).
  SELECT gate_id, lane_end, area_id
    INTO NEW.gate_id, NEW.gate_end, NEW.lane_id
  FROM public.classify_gate(NEW.geom)
  WHERE TRUE                                             -- this query overwrites NEW.lane_id if gate belongs to a lane; OK—consistent with gate
  LIMIT 1;                                               -- do nothing if no gate was intersected

  RETURN NEW;                                            -- row proceeds to insert with derived flags attached
END
$$;

-- AFTER INSERT trigger: compare with previous fix for this track and emit port/approach/lane events.
CREATE OR REPLACE FUNCTION public.trg_after_emit_events()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  prev RECORD;                                           -- previous fix for the same track
  vkey text;                                             -- track key we use to look up prev (prefer vessel_uid; else sat_track_uid)
BEGIN
  -- Decide which identity we use to thread fixes; vessel_uid preferred, otherwise SAT track continuity.
  vkey := COALESCE(NEW.vessel_uid, NEW.sat_track_uid);   -- if both NULL, we won't find a previous fix

  -- Load the previous fix strictly earlier in time for the same key (if any).
  IF vkey IS NOT NULL THEN
    SELECT *
      INTO prev
      FROM public.ais_fix p
      WHERE COALESCE(p.vessel_uid, p.sat_track_uid) = vkey
        AND p.ts < NEW.ts
      ORDER BY p.ts DESC
      LIMIT 1;                                           -- latest prior fix for this track
  END IF;

  -- Only emit transitions when we have a previous state to compare to.
  IF NOT FOUND THEN
    RETURN NULL;                                         -- first sighting: no transitions to compute
  END IF;

  -- ========== APPROACH ENTER ==========
  IF (COALESCE(prev.in_approach,false) = FALSE) AND (COALESCE(NEW.in_approach,false) = TRUE) THEN
    INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
    VALUES (NEW.ts, NEW.vessel_uid, NEW.sat_track_uid, 'approach_enter',
            COALESCE(NEW.area_id_approach, prev.area_id_approach), 'anchorage', NULL, NEW.lat, NEW.lon,
            jsonb_build_object('src', NEW.src));         -- entering anchorage polygon
  END IF;

  -- ========== APPROACH EXIT ==========
  IF (COALESCE(prev.in_approach,false) = TRUE) AND (COALESCE(NEW.in_approach,false) = FALSE) THEN
    INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
    VALUES (NEW.ts, NEW.vessel_uid, NEW.sat_track_uid, 'approach_exit',
            COALESCE(prev.area_id_approach, NEW.area_id_approach), 'anchorage', NULL, NEW.lat, NEW.lon,
            jsonb_build_object('src', NEW.src));         -- leaving anchorage polygon
  END IF;

  -- ========== PORT ENTER ==========
  IF (COALESCE(prev.in_core,false) = FALSE) AND (COALESCE(NEW.in_core,false) = TRUE) THEN
    INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
    VALUES (NEW.ts, NEW.vessel_uid, NEW.sat_track_uid, 'port_enter',
            COALESCE(NEW.area_id_core, prev.area_id_core), 'port', NULL, NEW.lat, NEW.lon,
            jsonb_build_object('src', NEW.src));         -- entering port harbor polygon
  END IF;

  -- ========== PORT EXIT ==========
  IF (COALESCE(prev.in_core,false) = TRUE) AND (COALESCE(NEW.in_core,false) = FALSE) THEN
    INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
    VALUES (NEW.ts, NEW.vessel_uid, NEW.sat_track_uid, 'port_exit',
            COALESCE(prev.area_id_core, NEW.area_id_core), 'port', NULL, NEW.lat, NEW.lon,
            jsonb_build_object('src', NEW.src));         -- leaving port harbor polygon
  END IF;

  -- ========== LANE ENTER (gate + outside→inside lane) ==========
  IF NEW.gate_id IS NOT NULL                                  -- we are in a gate polygon now
     AND (COALESCE(prev.in_lane,false) = FALSE)                -- previously outside the lane corridor
     AND (COALESCE(NEW.in_lane,false) = TRUE)                  -- now inside the lane corridor
  THEN
    INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
    VALUES (NEW.ts, NEW.vessel_uid, NEW.sat_track_uid, 'lane_enter',
            NEW.lane_id, 'lane', NEW.gate_end, NEW.lat, NEW.lon,
            jsonb_build_object('src', NEW.src, 'gate_id', NEW.gate_id)); -- entering the lane via this gate end
  END IF;

  -- ========== LANE EXIT (gate + inside→outside lane) ==========
  IF NEW.gate_id IS NOT NULL                                  -- we are in a gate polygon now
     AND (COALESCE(prev.in_lane,false) = TRUE)                 -- previously inside the lane corridor
     AND (COALESCE(NEW.in_lane,false) = FALSE)                 -- now outside the lane corridor
  THEN
    INSERT INTO public.ais_event(ts, vessel_uid, sat_track_uid, event, area_id, area_kind, gate_end, lat, lon, meta)
    VALUES (NEW.ts, NEW.vessel_uid, NEW.sat_track_uid, 'lane_exit',
            COALESCE(prev.lane_id, NEW.lane_id), 'lane', NEW.gate_end, NEW.lat, NEW.lon,
            jsonb_build_object('src', NEW.src, 'gate_id', NEW.gate_id)); -- exiting the lane via this gate end
  END IF;

  RETURN NULL;                                               -- AFTER triggers return NULL (row already inserted)
END
$$;

-- Hook the BEFORE trigger to compute geom + classifications ahead of insert.
DROP TRIGGER IF EXISTS trg_before_classify_fix ON public.ais_fix;                -- idempotent drop
CREATE TRIGGER trg_before_classify_fix
BEFORE INSERT ON public.ais_fix
FOR EACH ROW
EXECUTE FUNCTION public.trg_before_classify_fix();                                -- fills geom + membership + gate

-- Hook the AFTER trigger to emit events based on state changes at insert time.
DROP TRIGGER IF EXISTS trg_after_emit_events ON public.ais_fix;                   -- idempotent drop
CREATE TRIGGER trg_after_emit_events
AFTER INSERT ON public.ais_fix
FOR EACH ROW
EXECUTE FUNCTION public.trg_after_emit_events();                                  -- writes ais_event rows
