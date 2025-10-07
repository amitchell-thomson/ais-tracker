\pset pager off
\pset footer off
\pset linestyle ascii
\pset border 2

-- 1) Materialized views
SELECT schemaname, matviewname
FROM pg_matviews
ORDER BY 1,2;

-- 2) Continuous aggregates (columns that exist across TS versions)
SELECT view_schema, view_name, materialized_only
FROM timescaledb_information.continuous_aggregates
ORDER BY 1,2;

-- 3) Recent row counts (guard objects that may not exist yet)
-- Helper: existence flags -> psql variables

-- Counts for the core objects (these should exist after caggs)
SELECT 'ca_port_lifts_daily'   AS rel, COUNT(*) AS rows
  FROM public.ca_port_lifts_daily      WHERE day >= now() - interval '7 days'
UNION ALL
SELECT 'ca_lane_transit_daily',        COUNT(*)    FROM public.ca_lane_transit_daily    WHERE day >= now() - interval '7 days'
UNION ALL
SELECT 'ca_ballast_return_daily',      COUNT(*)    FROM public.ca_ballast_return_daily  WHERE day >= now() - interval '7 days'
UNION ALL
SELECT 'ca_class_mix_daily',           COUNT(*)    FROM public.ca_class_mix_daily       WHERE day >= now() - interval '7 days'
UNION ALL
SELECT 'mv_lane_transit_time_daily',   COUNT(*)    FROM public.mv_lane_transit_time_daily WHERE day >= now() - interval '7 days'
UNION ALL
SELECT 'mv_area_occupancy_daily',      COUNT(*)    FROM public.mv_area_occupancy_daily    WHERE day >= now() - interval '7 days';

