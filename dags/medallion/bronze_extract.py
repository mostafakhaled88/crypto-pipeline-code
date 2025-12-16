import requests
import json
import time
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger("airflow.task")
CONFIG_PATH = "/opt/airflow/config/api_config.json"

def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Failed to load config file: {e}")
        raise

def extract_and_load_bronze(ds: str):
    """
    Extract crypto prices from Coingecko API and load raw JSON into Bronze layer.
    Idempotent on logical date.
    """
    log.info(f"[{ds}] Starting Bronze Extraction")

    config = load_config()
    coingecko_cfg = config["api_source"]["coingecko"]
    vs_currencies = coingecko_cfg.get("vs_currencies", ["usd"]) # Default to ["usd"] if key is missing
    bronze_table = config["data_warehouse"]["bronze_table"]

    # دمج العملات في string واحد
    vs_currency_str = ",".join(vs_currencies)

    # Build API request
    coin_ids = ",".join(c["id"] for c in coingecko_cfg["coins_to_track"])
    params = {
        "ids": coin_ids,
        "vs_currencies": vs_currency_str
    }

    url = f"{coingecko_cfg['base_url']}{coingecko_cfg['endpoints']['simple_price']}"
    log.info(f"Requesting data from {url} | Coins: {coin_ids}")

    # Rate limiting
    time.sleep(coingecko_cfg.get("rate_limit", {}).get("sleep_seconds_min", 0))

    # API call with simple retry
    for attempt in range(config.get("pipeline_settings", {}).get("retry_attempts", 3)):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
            break
        except Exception as e:
            log.warning(f"API attempt {attempt + 1} failed: {e}")
            if attempt == config.get("pipeline_settings", {}).get("retry_attempts", 3) - 1:
                raise
            time.sleep(config.get("pipeline_settings", {}).get("retry_delay_seconds", 2))

    log.info(f"Extracted {len(raw_data)} coins")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn, conn.cursor() as cur:

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {bronze_table} (
                -- Remove the 'id' column entirely if it's not needed, 
                -- or keep it and change the primary key definition.
                
                -- Since you are making the insert idempotent on logical_date, 
                -- this column should be the PRIMARY KEY.
                logical_date DATE PRIMARY KEY, 
                raw_json_payload JSONB NOT NULL,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

# The INSERT statement remains the same and will now work:
# ON CONFLICT (logical_date) DO NOTHING;

        cur.execute(
            f"""
            INSERT INTO {bronze_table} (logical_date, raw_json_payload)
                    VALUES (%s, %s)
                    ON CONFLICT (logical_date) DO NOTHING;
            """,
            (ds, json.dumps(raw_data))
        )
        conn.commit()

        if cur.rowcount:
            log.info(f"[{ds}] Bronze data inserted")
        else:
            log.info(f"[{ds}] Data already exists – skipping")
