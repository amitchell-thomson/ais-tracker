# collector/maintenance.py
from sqlalchemy import create_engine, text

ENGINE = create_engine(
    "postgresql+psycopg2://ais:aispass@localhost:5432/ais",
    future=True,
)

def refresh_all(backfill_days: int = 2) -> None:
    """
    Force-refresh recent CAGG windows and dependent MVs.
    Must run in AUTOCOMMIT; do not wrap in a transaction.
    """
    with ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # Safer runtime settings (optional)
        conn.execute(text("SET TIME ZONE 'UTC'"))
        conn.execute(text("SET LOCAL lock_timeout = '2s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))

        # Time window: [start_of_day(now - N days), start_of_tomorrow)
        win_sql = """
        SELECT
          date_trunc('day', now() - make_interval(days => :days)) AS d0,
          date_trunc('day', now()) + interval '1 day'             AS d1
        """
        d0, d1 = conn.execute(text(win_sql), {"days": backfill_days}).one()

        # Continuous aggregates (top-level CALLs; NO transaction)
        cagg_tables = [
            "public.ca_port_lifts_daily",
            "public.ca_lane_transit_daily",
            "public.ca_ballast_return_daily",
            "public.ca_class_mix_daily",
        ]
        for cagg in cagg_tables:
            conn.execute(
                text(f"CALL refresh_continuous_aggregate('{cagg}', :d0, :d1)")
                .execution_options(autocommit=True),
                {"d0": d0, "d1": d1},
            )

        # Regular MVs (must be CONCURRENTLY and autocommit)
        mv_names = [
            "public.mv_lane_transit_time_daily",
            "public.mv_area_occupancy_daily",
        ]
        for mv in mv_names:
            conn.execute(
                text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                .execution_options(autocommit=True)
            )
