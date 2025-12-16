from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import json
from psycopg2.extras import execute_batch # Import moved here for clarity

log = logging.getLogger("airflow.task")

CONFIG_PATH = "/opt/airflow/config/api_config.json"


def load_config():
    """Load configuration from the file path."""
    # Added error handling for file loading
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Failed to load config file at {CONFIG_PATH}: {e}")
        raise


def transform_and_load_silver(ds: str):
    """
    Reads raw JSON from Bronze, flattens it, and loads clean, structured data 
    into the Silver table. Idempotent on (coin_id, vs_currency, source_timestamp).
    """
    log.info(f"[{ds}] Starting Silver Transformation")

    config = load_config()
    data_cfg = config["data_warehouse"]
    bronze_table = data_cfg.get("bronze_table", "bronze_raw_prices")
    silver_table = data_cfg.get("silver_table", "silver_clean_prices")
    
    # Use the vs_currencies defined in your config for transformation
    coingecko_cfg = config["api_source"]["coingecko"]
    vs_currencies = coingecko_cfg.get("vs_currencies", ["usd"])

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # 1️⃣ Read all Bronze records for the day
    # Retrieves the raw JSON payload and the `extracted_at` timestamp
    records = hook.get_records(
        f"""
        SELECT raw_json_payload, extracted_at
        FROM {bronze_table}
        WHERE logical_date = %s;
        """,
        parameters=(ds,)
    )

    if not records:
        log.warning(f"[{ds}] No Bronze data found – skipping Silver load.")
        return

    transformed_rows = []
    
    # 2️⃣ Flatten and Clean Data
    for raw_payload, extracted_at in records:
        # The raw_payload is already a parsed dictionary here due to hook.get_records returning a JSONB column
        for coin_id, prices in raw_payload.items():
            for currency in vs_currencies: # Iterate over ALL configured currencies (usd, eur, etc.)
                if currency in prices:
                    try:
                        # Prepare row: (coin_id, vs_currency, price, source_timestamp, processed_timestamp)
                        transformed_rows.append((
                            coin_id,
                            currency,
                            float(prices[currency]),
                            extracted_at,
                            datetime.utcnow() 
                        ))
                    except (ValueError, TypeError) as e:
                        # Handle case where price might be non-numeric (e.g., null or bad string)
                        log.warning(f"Skipping record for {coin_id}/{currency} due to bad price data: {e}")
                else:
                    log.debug(f"[{ds}] Skipping {coin_id} for currency {currency}: not found in payload.")

    if not transformed_rows:
        log.warning(f"[{ds}] No valid Silver records produced after cleaning.")
        return

    log.info(f"[{ds}] Prepared {len(transformed_rows)} Silver rows for upsert.")

    # 3️⃣ Load Data to Silver Table (Upsert)
    with hook.get_conn() as conn, conn.cursor() as cur:

        # Ensure Silver table exists. Added 'vs_currency' to the schema and PRIMARY KEY.
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {silver_table} (
                id SERIAL, -- Retained SERIAL ID, but not part of the unique key
                coin_id TEXT NOT NULL,
                vs_currency TEXT NOT NULL, 
                price NUMERIC NOT NULL,
                source_timestamp TIMESTAMP NOT NULL,
                processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (coin_id, vs_currency, source_timestamp) -- Use Primary Key for efficient ON CONFLICT reference
            );
        """)
        conn.commit()

        # Bulk upsert query
        execute_batch(
            cur,
            f"""
            INSERT INTO {silver_table}
            (coin_id, vs_currency, price, source_timestamp, processed_timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (coin_id, vs_currency, source_timestamp)
            DO UPDATE SET
                price = EXCLUDED.price,
                processed_timestamp = EXCLUDED.processed_timestamp;
            """,
            transformed_rows,
            page_size=100
        )
        conn.commit()

    log.info(f"[{ds}] Silver transformation completed successfully: {len(transformed_rows)} rows upserted.")