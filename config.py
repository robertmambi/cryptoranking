# config.py

from pathlib import Path

# Base directory (project root)
BASE_DIR = Path(__file__).resolve().parent

# API
API_KEY = "CG-oaJMTPzkRQ4cNRfbFVSNQdmL"

# Database
DB_PATH = BASE_DIR / "crypto.duckdb"
TABLE_NAME = "crypto_market"

# Logging
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "coingecko.log"

# CoinGecko
COINS_PER_RUN = 10000
