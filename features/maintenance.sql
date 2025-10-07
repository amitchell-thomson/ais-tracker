-- maintenance.sql
CREATE OR REPLACE PROCEDURE public.tick_refresh_all(
  p_backfill_days integer DEFAULT 2   -- how many recent days of CAGGs to force-refresh
)
LANGUAGE plpgsql
AS $$
DECLARE
  bucket interval := interval '1 day';
  d0     timestamptz;
  d1     timestamptz;
BEGIN
  -- Refresh a tight window so the latest buckets are finalized quickly.
  d0 := date_trunc('day', now() - make_interval(days => p_backfill_days));
  d1 := date_trunc('day', now()) + bucket;

  -- Continuous aggregates (explicit refresh on top of their policies)
  CALL refresh_continuous_aggregate('public.ca_port_lifts_daily',     d0, d1);
  CALL refresh_continuous_aggregate('public.ca_lane_transit_daily',   d0, d1);
  CALL refresh_continuous_aggregate('public.ca_ballast_return_daily', d0, d1);
  CALL refresh_continuous_aggregate('public.ca_class_mix_daily',      d0, d1);

  -- Regular MVs (must be refreshed explicitly)
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_lane_transit_time_daily;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_area_occupancy_daily;

  -- Features MV (built from the daily CAGGs/MVs)
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.ml_features_daily;
END;
$$;
