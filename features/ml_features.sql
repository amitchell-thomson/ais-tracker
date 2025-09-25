-- features_daily.sql
-- Materialized view that consolidates the EWMs into one daily feature table.

BEGIN;

-- Anchor calendar (days seen in any EWM)
WITH all_days AS (
  SELECT day FROM public.ca_port_lifts_ewm
  UNION
  SELECT day FROM public.ca_lane_transit_ewm
  UNION
  SELECT day FROM public.ca_transit_time_ewm
  UNION
  SELECT day FROM public.ca_anchorage_queue_ewm
  UNION
  SELECT day FROM public.ca_ballast_return_ewm
  UNION
  SELECT day FROM public.ca_class_mix_ewm
)
SELECT 1;  -- noop just for structure

DROP MATERIALIZED VIEW IF EXISTS public.ml_features_daily;
CREATE MATERIALIZED VIEW public.ml_features_daily AS
WITH days AS (
  SELECT DISTINCT day FROM (
    SELECT day FROM public.ca_port_lifts_ewm
    UNION SELECT day FROM public.ca_lane_transit_ewm
    UNION SELECT day FROM public.ca_transit_time_ewm
    UNION SELECT day FROM public.ca_anchorage_queue_ewm
    UNION SELECT day FROM public.ca_ballast_return_ewm
    UNION SELECT day FROM public.ca_class_mix_ewm
  ) u
),
-- US Export Pulse (Corpus + LOOP + Houston departures EWM)
us_export AS (
  SELECT day, sum(depart_dwt_ewm) AS us_export_pulse_ewm
  FROM public.ca_port_lifts_ewm t
  JOIN public.area a ON a.area_id=t.port_id
  WHERE a.name IN ('Corpus Christi - core // port','LOOP - core // port','Houston - core // port')
  GROUP BY day
),
-- EU Import Pulse (Rotterdam arrivals EWM)
eu_import AS (
  SELECT t.day, t.arrive_dwt_ewm AS eu_import_pulse_ewm
  FROM public.ca_port_lifts_ewm t
  JOIN public.area a ON a.area_id=t.port_id
  WHERE a.name='Rotterdam - core // port'
),
-- ME Export Pulse (Ras Tanura + Fujairah departures EWM)
me_export AS (
  SELECT day, sum(depart_dwt_ewm) AS me_export_pulse_ewm
  FROM public.ca_port_lifts_ewm t
  JOIN public.area a ON a.area_id=t.port_id
  WHERE a.name IN ('Ras Tanura - core // port','Fujairah - core // port')
  GROUP BY day
),
-- Routing share (Cape vs Mid-Atlantic laden DWT EWMs)
cape AS (
  SELECT day, sum(laden_dwt_sum_ewm) AS dwt
  FROM public.ca_lane_transit_ewm t
  JOIN public.area a ON a.area_id=t.lane_id
  WHERE a."group"='Cape of Good Hope'
  GROUP BY day
),
midatl AS (
  SELECT day, sum(laden_dwt_sum_ewm) AS dwt
  FROM public.ca_lane_transit_ewm t
  JOIN public.area a ON a.area_id=t.lane_id
  WHERE a."group"='Mid-Atlantic'
  GROUP BY day
),
routing AS (
  SELECT
    COALESCE(c.day, m.day) AS day,
    CASE WHEN COALESCE(c.dwt,0)+COALESCE(m.dwt,0) > 0
         THEN COALESCE(c.dwt,0)/(COALESCE(c.dwt,0)+COALESCE(m.dwt,0)) END AS share_asia_ewm
  FROM cape c FULL JOIN midatl m USING(day)
),
-- Transit time EWMs for key chokepoints
suez_tt AS (
  SELECT e.day, e.transit_time_p50_ewm AS transit_time_suez_p50_ewm
  FROM public.ca_transit_time_ewm e
  JOIN public.area a ON a.area_id=e.lane_id
  WHERE a.name='Suez - corridor // chokepoint'
),
hormuz_tt AS (
  SELECT e.day, e.transit_time_p50_ewm AS transit_time_hormuz_p50_ewm
  FROM public.ca_transit_time_ewm e
  JOIN public.area a ON a.area_id=e.lane_id
  WHERE a.name='Hormuz - corridor // chokepoint'
),
-- Anchorage queue EWMs
rtm_q AS (
  SELECT day, queue_cnt_ewm AS rotterdam_queue_ewm
  FROM public.ca_anchorage_queue_ewm q
  JOIN public.area a ON a.area_id=q.area_id
  WHERE a.name='Rotterdam - approach // port'
),
corpus_q AS (
  SELECT day, queue_cnt_ewm AS corpus_queue_ewm
  FROM public.ca_anchorage_queue_ewm q
  JOIN public.area a ON a.area_id=q.area_id
  WHERE a.name='Corpus Christi - approach // port'
),
ras_q AS (
  SELECT day, queue_cnt_ewm AS ras_tanura_queue_ewm
  FROM public.ca_anchorage_queue_ewm q
  JOIN public.area a ON a.area_id=q.area_id
  WHERE a.name='Ras Tanura - approach // port'
),
fuj_q AS (
  SELECT day, queue_cnt_ewm AS fujairah_queue_ewm
  FROM public.ca_anchorage_queue_ewm q
  JOIN public.area a ON a.area_id=q.area_id
  WHERE a.name='Fujairah - approach // port'
),
-- Ballast returns (sum across targeted westbound lanes)
ballast AS (
  SELECT day, sum(ballast_dwt_ewm) AS ballast_return_usgc_ewm
  FROM public.ca_ballast_return_ewm e
  JOIN public.area a ON a.area_id=e.lane_id
  WHERE a."group" IN ('Mid-Atlantic','Cape of Good Hope')
  GROUP BY day
),
-- Class mix share on Cape route (VLCC share as example)
cape_class AS (
  SELECT s.day,
         avg(s.vlcc_share_ewm) FILTER (WHERE a."group"='Cape of Good Hope') AS vlcc_share_cape_ewm
  FROM public.ca_class_mix_share_ewm s
  JOIN public.area a ON a.area_id=s.lane_id
  GROUP BY s.day
)
SELECT
  d.day,
  u.us_export_pulse_ewm,
  e.eu_import_pulse_ewm,
  m.me_export_pulse_ewm,
  r.share_asia_ewm,
  st.transit_time_suez_p50_ewm,
  ht.transit_time_hormuz_p50_ewm,
  rq.rotterdam_queue_ewm,
  cq.corpus_queue_ewm,
  raq.ras_tanura_queue_ewm,
  fq.fujairah_queue_ewm,
  b.ballast_return_usgc_ewm,
  cc.vlcc_share_cape_ewm
FROM days d
LEFT JOIN us_export  u  ON u.day  = d.day
LEFT JOIN eu_import  e  ON e.day  = d.day
LEFT JOIN me_export  m  ON m.day  = d.day
LEFT JOIN routing    r  ON r.day  = d.day
LEFT JOIN suez_tt    st ON st.day = d.day
LEFT JOIN hormuz_tt  ht ON ht.day = d.day
LEFT JOIN rtm_q      rq ON rq.day = d.day
LEFT JOIN corpus_q   cq ON cq.day = d.day
LEFT JOIN ras_q      raq ON raq.day = d.day
LEFT JOIN fuj_q      fq ON fq.day = d.day
LEFT JOIN ballast    b  ON b.day  = d.day
LEFT JOIN cape_class cc ON cc.day = d.day
ORDER BY d.day;

CREATE INDEX IF NOT EXISTS ix_ml_features_daily_day ON public.ml_features_daily(day);

COMMIT;

-- To refresh later:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY public.ml_features_daily;
