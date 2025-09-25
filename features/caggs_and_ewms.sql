-- caggs_and_ewm.sql
-- One-shot initializer: helpers, continuous aggregates, support MVs, EWMs, and policies.

-- Extensions (safe if already present)
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- Helper: α for daily EWM given a half-life
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.ewm_alpha_daily(halflife interval)
RETURNS double precision
LANGUAGE sql IMMUTABLE AS $$
  SELECT 1 - exp( -ln(2) * (extract(epoch from interval '1 day')
                          / NULLIF(extract(epoch from $1),0)) );
$$;

-- ---------------------------------------------------------------------------
-- 1) PORT LIFTS (daily, CAGG)  — from ais_event (hypertable)
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

-- EWM table + hypertable
CREATE TABLE IF NOT EXISTS public.ca_port_lifts_ewm (
  day timestamptz NOT NULL,
  port_id text NOT NULL,
  depart_dwt_ewm double precision,
  arrive_dwt_ewm double precision,
  PRIMARY KEY (day, port_id)
);
SELECT create_hypertable('public.ca_port_lifts_ewm','day', if_not_exists=>true);

-- Refresher: cumulatively builds EWM forward by port_id
CREATE OR REPLACE FUNCTION public.refresh_ca_port_lifts_ewm(
  p_from timestamptz,
  p_to   timestamptz,
  p_halflife interval DEFAULT '21 days')
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE a double precision := public.ewm_alpha_daily(p_halflife);
BEGIN
  INSERT INTO public.ca_port_lifts_ewm AS t
  SELECT
    d.day, d.port_id,
    a*COALESCE(d.depart_dwt_laden,0) + (1-a)*COALESCE(LAG(prev.depart_dwt_ewm) OVER w, 0) AS depart_dwt_ewm,
    a*COALESCE(d.arrive_dwt_laden,0) + (1-a)*COALESCE(LAG(prev.arrive_dwt_ewm) OVER w, 0) AS arrive_dwt_ewm
  FROM public.ca_port_lifts_daily d
  LEFT JOIN LATERAL (
    SELECT * FROM public.ca_port_lifts_ewm
    WHERE port_id=d.port_id AND day < d.day
    ORDER BY day DESC LIMIT 1
  ) prev ON TRUE
  WHERE d.day >= p_from AND d.day < p_to
  WINDOW w AS (PARTITION BY d.port_id ORDER BY d.day)
  ON CONFLICT (day, port_id) DO UPDATE
  SET depart_dwt_ewm = EXCLUDED.depart_dwt_ewm,
      arrive_dwt_ewm = EXCLUDED.arrive_dwt_ewm;
END; $$;

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

CREATE TABLE IF NOT EXISTS public.ca_lane_transit_ewm (
  day timestamptz NOT NULL,
  lane_id text NOT NULL,
  transit_cnt_ewm double precision,
  laden_dwt_sum_ewm double precision,
  PRIMARY KEY (day, lane_id)
);
SELECT create_hypertable('public.ca_lane_transit_ewm','day', if_not_exists=>true);

CREATE OR REPLACE FUNCTION public.refresh_ca_lane_transit_ewm(
  p_from timestamptz,
  p_to   timestamptz,
  p_halflife interval DEFAULT '14 days')
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE a double precision := public.ewm_alpha_daily(p_halflife);
BEGIN
  INSERT INTO public.ca_lane_transit_ewm AS t
  SELECT
    d.day, d.lane_id,
    a*COALESCE(d.transit_cnt,0) + (1-a)*COALESCE(LAG(prev.transit_cnt_ewm)   OVER w, 0) AS transit_cnt_ewm,
    a*COALESCE(d.laden_dwt_sum,0)+(1-a)*COALESCE(LAG(prev.laden_dwt_sum_ewm) OVER w, 0) AS laden_dwt_sum_ewm
  FROM public.ca_lane_transit_daily d
  LEFT JOIN LATERAL (
    SELECT * FROM public.ca_lane_transit_ewm
    WHERE lane_id=d.lane_id AND day < d.day
    ORDER BY day DESC LIMIT 1
  ) prev ON TRUE
  WHERE d.day >= p_from AND d.day < p_to
  WINDOW w AS (PARTITION BY d.lane_id ORDER BY d.day)
  ON CONFLICT (day, lane_id) DO UPDATE
  SET transit_cnt_ewm   = EXCLUDED.transit_cnt_ewm,
      laden_dwt_sum_ewm = EXCLUDED.laden_dwt_sum_ewm;
END; $$;

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

CREATE TABLE IF NOT EXISTS public.ca_transit_time_ewm (
  day timestamptz NOT NULL,
  lane_id text NOT NULL,
  transit_time_p50_ewm double precision,
  PRIMARY KEY (day, lane_id)
);
SELECT create_hypertable('public.ca_transit_time_ewm','day', if_not_exists=>true);

CREATE OR REPLACE FUNCTION public.refresh_ca_transit_time_ewm(
  p_from timestamptz,
  p_to   timestamptz,
  p_halflife interval DEFAULT '10 days')
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE a double precision := public.ewm_alpha_daily(p_halflife);
BEGIN
  -- Ensure the MV is up to date in the window
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_lane_transit_time_daily;

  INSERT INTO public.ca_transit_time_ewm AS t
  SELECT
    d.day, d.lane_id,
    a*COALESCE(d.transit_time_p50_s,0) + (1-a)*COALESCE(LAG(prev.transit_time_p50_ewm) OVER w, 0)
      AS transit_time_p50_ewm
  FROM public.mv_lane_transit_time_daily d
  LEFT JOIN LATERAL (
    SELECT * FROM public.ca_transit_time_ewm
    WHERE lane_id=d.lane_id AND day < d.day
    ORDER BY day DESC LIMIT 1
  ) prev ON TRUE
  WHERE d.day >= p_from AND d.day < p_to
  WINDOW w AS (PARTITION BY d.lane_id ORDER BY d.day)
  ON CONFLICT (day, lane_id) DO UPDATE
  SET transit_time_p50_ewm = EXCLUDED.transit_time_p50_ewm;
END; $$;

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

CREATE TABLE IF NOT EXISTS public.ca_anchorage_queue_ewm (
  day timestamptz NOT NULL,
  area_id text NOT NULL,
  queue_cnt_ewm double precision,
  PRIMARY KEY (day, area_id)
);
SELECT create_hypertable('public.ca_anchorage_queue_ewm','day', if_not_exists=>true);

CREATE OR REPLACE FUNCTION public.refresh_ca_anchorage_queue_ewm(
  p_from timestamptz,
  p_to   timestamptz,
  p_halflife interval DEFAULT '10 days')
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE a double precision := public.ewm_alpha_daily(p_halflife);
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_area_occupancy_daily;

  INSERT INTO public.ca_anchorage_queue_ewm AS t
  SELECT
    d.day, d.area_id,
    a*COALESCE(d.avg_open_sessions,0) + (1-a)*COALESCE(LAG(prev.queue_cnt_ewm) OVER w, 0) AS queue_cnt_ewm
  FROM public.mv_area_occupancy_daily d
  LEFT JOIN LATERAL (
    SELECT * FROM public.ca_anchorage_queue_ewm
    WHERE area_id=d.area_id AND day < d.day
    ORDER BY day DESC LIMIT 1
  ) prev ON TRUE
  WHERE d.day >= p_from AND d.day < p_to
  WINDOW w AS (PARTITION BY d.area_id ORDER BY d.day)
  ON CONFLICT (day, area_id) DO UPDATE
  SET queue_cnt_ewm = EXCLUDED.queue_cnt_ewm;
END; $$;

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

CREATE TABLE IF NOT EXISTS public.ca_ballast_return_ewm (
  day timestamptz NOT NULL,
  lane_id text NOT NULL,
  ballast_dwt_ewm double precision,
  PRIMARY KEY (day, lane_id)
);
SELECT create_hypertable('public.ca_ballast_return_ewm','day', if_not_exists=>true);

CREATE OR REPLACE FUNCTION public.refresh_ca_ballast_return_ewm(
  p_from timestamptz,
  p_to   timestamptz,
  p_halflife interval DEFAULT '14 days')
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE a double precision := public.ewm_alpha_daily(p_halflife);
BEGIN
  INSERT INTO public.ca_ballast_return_ewm AS t
  SELECT
    d.day, d.lane_id,
    a*COALESCE(d.ballast_dwt,0) + (1-a)*COALESCE(LAG(prev.ballast_dwt_ewm) OVER w, 0) AS ballast_dwt_ewm
  FROM public.ca_ballast_return_daily d
  LEFT JOIN LATERAL (
    SELECT * FROM public.ca_ballast_return_ewm
    WHERE lane_id=d.lane_id AND day < d.day
    ORDER BY day DESC LIMIT 1
  ) prev ON TRUE
  WHERE d.day >= p_from AND d.day < p_to
  WINDOW w AS (PARTITION BY d.lane_id ORDER BY d.day)
  ON CONFLICT (day, lane_id) DO UPDATE
  SET ballast_dwt_ewm = EXCLUDED.ballast_dwt_ewm;
END; $$;

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
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int >= 200000)     AS dwt_vlcc,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int BETWEEN 120000 AND 199999) AS dwt_suezmax,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int BETWEEN  80000 AND 119999) AS dwt_aframax,
  sum((e.meta->>'dwt')::int)
    FILTER (WHERE e.event='lane_exit' AND (e.meta->>'dwt')::int < 80000)       AS dwt_other
FROM public.ais_event e
WHERE e.area_kind IN ('lane','chokepoint')
GROUP BY 1,2;

SELECT add_continuous_aggregate_policy(
  'public.ca_class_mix_daily',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '30 minutes'
);

CREATE TABLE IF NOT EXISTS public.ca_class_mix_ewm (
  day timestamptz NOT NULL,
  lane_id text NOT NULL,
  dwt_vlcc_ewm double precision,
  dwt_suezmax_ewm double precision,
  dwt_aframax_ewm double precision,
  dwt_other_ewm double precision,
  PRIMARY KEY (day, lane_id)
);
SELECT create_hypertable('public.ca_class_mix_ewm','day', if_not_exists=>true);

CREATE OR REPLACE FUNCTION public.refresh_ca_class_mix_ewm(
  p_from timestamptz,
  p_to   timestamptz,
  p_halflife interval DEFAULT '14 days')
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE a double precision := public.ewm_alpha_daily(p_halflife);
BEGIN
  INSERT INTO public.ca_class_mix_ewm AS t
  SELECT
    d.day, d.lane_id,
    a*COALESCE(d.dwt_vlcc,0)    + (1-a)*COALESCE(LAG(prev.dwt_vlcc_ewm)    OVER w, 0) AS dwt_vlcc_ewm,
    a*COALESCE(d.dwt_suezmax,0) + (1-a)*COALESCE(LAG(prev.dwt_suezmax_ewm) OVER w, 0) AS dwt_suezmax_ewm,
    a*COALESCE(d.dwt_aframax,0) + (1-a)*COALESCE(LAG(prev.dwt_aframax_ewm) OVER w, 0) AS dwt_aframax_ewm,
    a*COALESCE(d.dwt_other,0)   + (1-a)*COALESCE(LAG(prev.dwt_other_ewm)   OVER w, 0) AS dwt_other_ewm
  FROM public.ca_class_mix_daily d
  LEFT JOIN LATERAL (
    SELECT * FROM public.ca_class_mix_ewm
    WHERE lane_id=d.lane_id AND day < d.day
    ORDER BY day DESC LIMIT 1
  ) prev ON TRUE
  WHERE d.day >= p_from AND d.day < p_to
  WINDOW w AS (PARTITION BY d.lane_id ORDER BY d.day)
  ON CONFLICT (day, lane_id) DO UPDATE
  SET dwt_vlcc_ewm    = EXCLUDED.dwt_vlcc_ewm,
      dwt_suezmax_ewm = EXCLUDED.dwt_suezmax_ewm,
      dwt_aframax_ewm = EXCLUDED.dwt_aframax_ewm,
      dwt_other_ewm   = EXCLUDED.dwt_other_ewm;
END; $$;

-- Convenience view: class shares from EWMs
CREATE OR REPLACE VIEW public.ca_class_mix_share_ewm AS
WITH base AS (
  SELECT day, lane_id,
         COALESCE(dwt_vlcc_ewm,0)    AS vlcc,
         COALESCE(dwt_suezmax_ewm,0) AS suez,
         COALESCE(dwt_aframax_ewm,0) AS afra,
         COALESCE(dwt_other_ewm,0)   AS oth
  FROM public.ca_class_mix_ewm
)
SELECT
  day, lane_id,
  CASE WHEN (vlcc+suez+afra+oth) > 0 THEN vlcc/(vlcc+suez+afra+oth) END AS vlcc_share_ewm,
  CASE WHEN (vlcc+suez+afra+oth) > 0 THEN suez/(vlcc+suez+afra+oth) END AS suezmax_share_ewm,
  CASE WHEN (vlcc+suez+afra+oth) > 0 THEN afra/(vlcc+suez+afra+oth) END AS aframax_share_ewm
FROM base;



-- ---------------------------------------------------------------------------
-- Backfill + EWM refresh helpers (run once after load; keep or remove)
-- Backfill CAGGs (must cover at least one full 1-day bucket) and seed EWMs
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  bucket interval := interval '1 day';
  d0     timestamptz;
  d1     timestamptz;
BEGIN
  -- Start at the earliest event day (minus one bucket) or fall back to 180 days ago
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

  -- Refresh non-CAGG MVs that the EWMs depend on
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_lane_transit_time_daily;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_area_occupancy_daily;

  -- Seed EWMs across the same window (tune halflives if you like)
  PERFORM public.refresh_ca_port_lifts_ewm(     d0, d1, interval '21 days');
  PERFORM public.refresh_ca_lane_transit_ewm(   d0, d1, interval '14 days');
  PERFORM public.refresh_ca_transit_time_ewm(   d0, d1, interval '10 days');
  PERFORM public.refresh_ca_anchorage_queue_ewm(d0, d1, interval '10 days');
  PERFORM public.refresh_ca_ballast_return_ewm( d0, d1, interval '14 days');
  PERFORM public.refresh_ca_class_mix_ewm(      d0, d1, interval '14 days');
END $$;

