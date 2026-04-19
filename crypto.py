# crypto.py

import time
import requests
import duckdb
import logging
from datetime import date
from config import (
    API_KEY,
    DB_PATH,
    TABLE_NAME,
    LOG_DIR,
    LOG_FILE,
    COINS_PER_RUN,
)
from query_functions import (
    wait_for_network,
    run_rank_improvement_events,
)




# -------------------------------------------------
# Logging setup (file + console)
# -------------------------------------------------
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)

# -------------------------------------------------
# CoinGecko config
# -------------------------------------------------
URL = "https://api.coingecko.com/api/v3/coins/markets"

HEADERS = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY,   # use x-cg-pro-api-key if Pro
}

BASE_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 250,
    "sparkline": False,
}

# -------------------------------------------------
# Fetch snapshot (page by page)
# -------------------------------------------------
def fetch_pages(max_coins):
    snapshot_date = date.today()
    pages = (max_coins // 250) + (1 if max_coins % 250 else 0)

    for page in range(1, pages + 1):
        try:
            params = BASE_PARAMS | {"page": page}

            response = requests.get(
                URL, headers=HEADERS, params=params, timeout=30
            )
            response.raise_for_status()

            data = response.json()
            rows = []

            for coin in data:
                market_cap_m = (
                    round(coin["market_cap"] / 1_000_000, 2)
                    if coin.get("market_cap") is not None
                    else None
                )

                symbol = coin.get("symbol")
                cid = coin.get("id")

                # Your canonical ID
                id_ = f"{symbol}_{cid}"

                rows.append((
                    snapshot_date,
                    id_,
                    cid,
                    symbol,
                    coin.get("market_cap_rank"),
                    coin.get("current_price"),
                    market_cap_m,
                ))

           # logging.info(f"Fetched page {page} ({len(rows)} coins)")
            yield page, rows

            # ✅ delay 1 second before next request
            time.sleep(1)

        except Exception:
            logging.exception(f"Failed fetching page {page}")
            raise

# -------------------------------------------------
# Save to DuckDB (skip duplicates safely)
# -------------------------------------------------
def save_to_duckdb(paged_rows):
    con = duckdb.connect(DB_PATH)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            snapshot_date DATE,
            id TEXT,
            cid TEXT,
            symbol TEXT,
            rank INTEGER,
            price DOUBLE,
            market_cap_m DOUBLE,
            UNIQUE (snapshot_date, id)
        )
    """)

    total_inserted = 0

    for page, rows in paged_rows:
        snapshot_date = rows[0][0]

        existing_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM {TABLE_NAME}
            WHERE snapshot_date = ?
              AND id IN ({",".join(["?"] * len(rows))})
            """,
            [snapshot_date] + [r[1] for r in rows],
        ).fetchone()[0]

        con.executemany(
            f"""
            INSERT INTO {TABLE_NAME} (
                snapshot_date, id, cid, symbol, rank, price, market_cap_m
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM {TABLE_NAME}
                WHERE snapshot_date = ?
                  AND id = ?
            )
            """,
            [
                (
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                    r[0], r[1],
                )
                for r in rows
            ],
        )

        inserted = len(rows) - existing_count
        total_inserted += inserted

        logging.info(
            f"Page {page}: fetched {len(rows)}, inserted {inserted}"
        )

    con.close()
    logging.info(f"Total inserted today: {total_inserted}")

# -------------------------------------------------
# Main
# -------------------------------------------------
if __name__ == "__main__":
    logging.info("===== CoinGecko daily snapshot START =====")

    if not wait_for_network(retries=10, delay=20):
        logging.error("CoinGecko snapshot aborted due to network issue.")
        raise SystemExit(1)

    try:
        paged_rows = fetch_pages(COINS_PER_RUN)
        save_to_duckdb(paged_rows)

        run_rank_improvement_events()

        logging.info("===== CoinGecko daily snapshot END =====")

    except Exception:
        logging.exception("CoinGecko snapshot FAILED")
        raise

