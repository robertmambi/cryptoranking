# query_functions.py

import duckdb
import socket
import time
import logging
from config import DB_PATH

def wait_for_network(
    host="api.coingecko.com",
    port=443,
    retries=10,
    delay=10
):
    """
    Wait for network/DNS availability.

    Returns True if network becomes available, False otherwise.
    """
    for attempt in range(1, retries + 1):
        try:
            socket.getaddrinfo(host, port)
            logging.info("Network available")
            return True
        except socket.gaierror as e:
            logging.warning(
                f"Network not available (attempt {attempt}/{retries}): {e}"
            )
            time.sleep(delay)

    logging.error(
        f"Network still unavailable after {retries} attempts. Aborting run."
    )
    return False


def run_rank_improvement_events():
    """
    Logic:
    - First occurrence per id -> TRUE
    - Rank lower than ALL previous -> TRUE
    - Rank equal or higher -> FALSE
    """

    con = duckdb.connect(DB_PATH)

    con.execute("""
        CREATE TABLE IF NOT EXISTS rank_improvement_events (
            snapshot_date DATE,
            id TEXT,
            cid TEXT,
            symbol TEXT,
            rank INTEGER,
            prev_best_rank INTEGER,
            UNIQUE (snapshot_date, id)
        )
    """)

    con.execute("""
        INSERT INTO rank_improvement_events
        SELECT *
        FROM (
            SELECT
                snapshot_date,
                id,
                cid,
                symbol,
                rank,
                MIN(rank) OVER (
                    PARTITION BY id
                    ORDER BY snapshot_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS prev_best_rank
            FROM crypto_market
        ) s
        WHERE
            (
                s.prev_best_rank IS NULL
                OR s.rank < s.prev_best_rank
            )
            AND NOT EXISTS (
                SELECT 1
                FROM rank_improvement_events t
                WHERE t.snapshot_date = s.snapshot_date
                  AND t.id = s.id
            )
    """)

    con.close()

