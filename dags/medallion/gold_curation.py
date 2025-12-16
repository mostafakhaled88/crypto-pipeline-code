from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import json

log = logging.getLogger("airflow.task")

CONFIG_PATH = "/opt/airflow/config/api_config.json"


def load_config():
    """Load configuration from the file path."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Failed to load config file at {CONFIG_PATH}: {e}")
        raise


def curate_gold_metrics(ds: str):
    """
    Aggregate Silver layer into daily Gold metrics (AVG, MIN, MAX, Daily Change).
    Idempotent on (metric_date, coin_id, vs_currency).
    """
    log.info(f"[{ds}] Starting Gold Curation")

    config = load_config()
    data_cfg = config["data_warehouse"]
    silver_table = data_cfg.get("silver_table", "silver_clean_prices")
    gold_table = data_cfg.get("gold_table", "gold_daily_metrics")

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # 🛑 FIX APPLIED HERE: Changed hook.run(..., fetch=True) to hook.get_first()
    # The date for the previous execution date (ds - 1 day)
    prev_ds = hook.get_first("SELECT (%s::date - INTERVAL '1 day')::date", parameters=(ds,))[0]

    with hook.get_conn() as conn, conn.cursor() as cur:

        # 1️⃣ Ensure Gold table exists (Schema from previous step)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {gold_table} (
                metric_date DATE NOT NULL,
                coin_id TEXT NOT NULL,
                vs_currency TEXT NOT NULL,
                avg_price NUMERIC,
                min_price NUMERIC,
                max_price NUMERIC,
                daily_change_pct NUMERIC,
                PRIMARY KEY (metric_date, coin_id, vs_currency)
            );
        """)
        conn.commit()

        # 2️⃣ Aggregate + upsert using CTE to calculate daily change
        sql = f"""
            WITH daily_agg AS (
                -- Calculate current day's metrics
                SELECT
                    %s::date AS metric_date,
                    coin_id,
                    vs_currency,
                    AVG(price) AS avg_price,
                    MIN(price) AS min_price,
                    MAX(price) AS max_price
                FROM {silver_table}
                -- Filter data for the current logical date (ds)
                WHERE source_timestamp >= %s::date
                  AND source_timestamp < (%s::date + INTERVAL '1 day')
                GROUP BY coin_id, vs_currency
            ),
            
            prev_day_agg AS (
                -- Retrieve previous day's AVG price for calculating daily change
                SELECT
                    coin_id,
                    vs_currency,
                    avg_price
                FROM {gold_table}
                WHERE metric_date = %s::date
            )
            
            INSERT INTO {gold_table} 
                (metric_date, coin_id, vs_currency, avg_price, min_price, max_price, daily_change_pct)
            SELECT
                t1.metric_date,
                t1.coin_id,
                t1.vs_currency,
                t1.avg_price,
                t1.min_price,
                t1.max_price,
                -- Calculate Percentage Change: (Current_AVG - Prev_AVG) / Prev_AVG * 100
                (t1.avg_price - t2.avg_price) / t2.avg_price * 100 AS daily_change_pct
            FROM daily_agg t1
            LEFT JOIN prev_day_agg t2 
                ON t1.coin_id = t2.coin_id 
                AND t1.vs_currency = t2.vs_currency
            
            ON CONFLICT (metric_date, coin_id, vs_currency) DO UPDATE
            SET 
                avg_price = EXCLUDED.avg_price,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                daily_change_pct = EXCLUDED.daily_change_pct;
        """

        # Execute the query, passing the current date (ds) three times and previous date once
        cur.execute(sql, (ds, ds, ds, prev_ds))

        affected = cur.rowcount
        conn.commit()

    log.info(f"[{ds}] Gold curation completed. Rows affected: {affected}")
    return affected