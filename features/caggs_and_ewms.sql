-- caggs_and_ewm.sql  (EWMs REMOVED)
-- One-shot initializer: extensions, continuous aggregates, support MVs, and policies (no EWM state).

-- ---------------------------------------------------------------------------
-- Extensions (safe if already present)
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- 1) PORT LIFTS (daily, CAGG) — from ais_event (hypertable)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.ca_port_lifts_daily;
CREATE MATERIALIZED VIEW public.ca_port_lifts_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', e.ts)                    AS day,
  e.area_id                                     AS port_id,
  a.name                                        AS port_name,
  COALESCE(a.flow_role,'mixed')                 AS flow_role,
  -- Laden exports (departures)
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='port_exit'
                 AND COALESCE(
                       (e.meta->>'laden')::bool,
                       CASE a.flow_role WHEN 'export' THEN true
                                        WHEN 'import' THEN false
                                        ELSE NULL END
                     ) IS TRUE)                 AS depart_dwt_laden,
  -- Laden imports (arrivals)
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='port_enter'
                 AND COALESCE(
                       (e.meta->>'laden')::bool,
                       CASE a.flow_role WHEN 'import' THEN true
                                        WHEN 'export' THEN false
                                        ELSE NULL END
                     ) IS TRUE)                 AS arrive_dwt_laden
FROM public.ais_event e
JOIN public.area a ON a.area_id = e.area_id
WHERE e.area_kind='port'
GROUP BY 1,2,3,4;

-- Policy to keep it fresh
SELECT add_continuous_aggregate_policy(
  'public.ca_port_lifts_daily',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '30 minutes'
);

-- ---------------------------------------------------------------------------
-- 2) LANE TRANSITS (daily, CAGG) — counts + laden DWT
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.ca_lane_transit_daily;
CREATE MATERIALIZED VIEW public.ca_lane_transit_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', e.ts) AS day,
  e.area_id                  AS lane_id,
  a.name                     AS lane_name,
  count(*) FILTER (WHERE e.event='lane_exit')                                AS transit_cnt,
  sum((e.meta->>'dwt')::int) FILTER (WHERE e.event='lane_exit'
                                       AND (e.meta->>'laden')::bool IS TRUE) AS laden_dwt_sum
FROM public.ais_event e
JOIN public.area a ON a.area_id = e.area_id
WHERE e.area_kind IN ('lane','chokepoint')
GROUP BY 1,2,3;

SELECT add_continuous_aggregate_policy(
  'public.ca_lane_transit_daily',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '30 minutes'
);

-- ---------------------------------------------------------------------------
-- 3) LANE TRANSIT TIMES (daily, MV) — from vessel_dwell_session (regular table)
--     (Not a CAGG because source is not a hypertable.)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.mv_lane_transit_time_daily;
CREATE MATERIALIZED VIEW public.mv_lane_transit_time_daily AS
SELECT
  time_bucket('1 day', end_ts) AS day,
  area_id                      AS lane_id,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY duration_s) AS transit_time_p50_s,
  avg(duration_s)                                         AS transit_time_mean_s
FROM public.vessel_dwell_session
WHERE area_kind IN ('lane','chokepoint') AND is_open = false AND end_ts IS NOT NULL
GROUP BY 1,2;

-- Needed for CONCURRENTLY refreshes
CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_lane_transit_time_daily
  ON public.mv_lane_transit_time_daily (day, lane_id);

CREATE INDEX IF NOT EXISTS ix_mv_lane_transit_time_daily_day_lane
  ON public.mv_lane_transit_time_daily(day, lane_id);

-- ---------------------------------------------------------------------------
-- 4) ANCHORAGE QUEUES (daily, MV) — approach occupancy proxy
--     (We aggregate 5-min snapshots derived from sessions; MV not CAGG.)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.mv_area_occupancy_daily;
CREATE MATERIALIZED VIEW public.mv_area_occupancy_daily AS
WITH snaps AS (
  SELECT
    time_bucket('5 minutes', COALESCE(end_ts, start_ts)) AS ts,
    area_id, area_kind,
    max(is_open::int) AS open_sessions
  FROM public.vessel_dwell_session
  GROUP BY 1,2,3
)
SELECT
  time_bucket('1 day', ts) AS day,
  area_id,
  avg(open_sessions)::double precision AS avg_open_sessions
FROM snaps
JOIN public.area a USING (area_id)
WHERE a.kind='port' AND a.subtype='approach'
GROUP BY 1,2;

-- Needed for CONCURRENTLY refreshes
CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_area_occupancy_daily
  ON public.mv_area_occupancy_daily (day, area_id);

CREATE INDEX IF NOT EXISTS ix_mv_area_occupancy_daily_day_area
  ON public.mv_area_occupancy_daily(day, area_id);

-- ---------------------------------------------------------------------------
-- 5) BALLAST RETURNS (daily, CAGG) — unlade exits heading west on target lanes
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.ca_ballast_return_daily;
CREATE MATERIALIZED VIEW public.ca_ballast_return_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', e.ts) AS day,
  e.area_id                  AS lane_id,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit'
                 AND (e.meta->>'laden')::bool IS FALSE
                 AND e.gate_end='west') AS ballast_dwt
FROM public.ais_event e
JOIN public.area a ON a.area_id = e.area_id
WHERE e.area_kind IN ('lane','chokepoint')
  AND a."group" IN ('Mid-Atlantic','Cape of Good Hope')   -- tune to your taxonomy
GROUP BY 1,2;

SELECT add_continuous_aggregate_policy(
  'public.ca_ballast_return_daily',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '30 minutes'
);

-- ---------------------------------------------------------------------------
-- 6) CLASS MIX (daily, CAGG) — DWT buckets by class
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.ca_class_mix_daily;
CREATE MATERIALIZED VIEW public.ca_class_mix_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', e.ts) AS day,
  e.area_id                  AS lane_id,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int >= 200000)                   AS dwt_vlcc,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int BETWEEN 120000 AND 199999)   AS dwt_suezmax,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int BETWEEN  80000 AND 119999)   AS dwt_aframax,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int < 80000)                     AS dwt_other
FROM public.ais_event e
WHERE e.area_kind IN ('lane','chokepoint')
GROUP BY 1,2;

SELECT add_continuous_aggregate_policy(
  'public.ca_class_mix_daily',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '30 minutes'
);

-- ---------------------------------------------------------------------------
-- Backfill helpers (run once after load)
--   Refresh CAGGs over a historical window and bring MVs up to date.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  bucket interval := interval '1 day';
  d0     timestamptz;
  d1     timestamptz;
BEGIN
  -- Start at earliest event day (minus one bucket) or fall back to 180 days ago
  SELECT COALESCE(
           date_trunc('day', (SELECT min(ts) FROM public.ais_event)) - bucket,
           date_trunc('day', now()) - interval '180 days'
         )
    INTO d0;

  -- End at tomorrow midnight (covers today's bucket fully)
  d1 := date_trunc('day', now()) + bucket;

  -- Refresh all daily CAGGs over [d0, d1)
  CALL refresh_continuous_aggregate('public.ca_port_lifts_daily',      d0, d1);
  CALL refresh_continuous_aggregate('public.ca_lane_transit_daily',    d0, d1);
  CALL refresh_continuous_aggregate('public.ca_ballast_return_daily',  d0, d1);
  CALL refresh_continuous_aggregate('public.ca_class_mix_daily',       d0, d1);

  -- Refresh non-CAGG MVs that downstream analysis depends on
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_lane_transit_time_daily;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_area_occupancy_daily;
END $$;
