#!/usr/bin/env python3
"""
Simple ETL: Google Sheets -> PostgreSQL

- Authenticates to Google Sheets via a Service Account JSON (provided via env var)
- Reads a worksheet into a pandas DataFrame
- Loads the data into a PostgreSQL table

Environment variables:
  SHEET_ID
  WORKSHEET
  GOOGLE_SA_KEY_JSON   # full JSON (GitHub Secret) OR use GOOGLE_SA_KEY_PATH locally
  GOOGLE_SA_KEY_PATH   # path to service account .json (local .env)
  PG_USER
  PG_PASSWORD
  PG_HOST
  PG_PORT
  PG_DB
  PG_TABLE -> Provide whatever name you want
  PG_SSLMODE  # optional, e.g. require (Supabase)
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("etl")

# ─── Config helpers ───────────────────────────────────────────────────────────
def load_local_env() -> None:
    """Charge `.env` à côté de etl.py (dev local uniquement)."""
    if load_dotenv is None:
        return
    load_dotenv(Path(__file__).resolve().parent / ".env")


def resolve_google_sa_json() -> str:
    inline = os.getenv("GOOGLE_SA_KEY_JSON", "").strip()
    if inline:
        return inline
    key_path = os.getenv("GOOGLE_SA_KEY_PATH", "").strip()
    if key_path:
        path = Path(key_path).expanduser()
        if not path.is_absolute():
            path = Path(__file__).resolve().parent / path
        if not path.is_file():
            raise RuntimeError(f"GOOGLE_SA_KEY_PATH introuvable: {path}")
        return path.read_text(encoding="utf-8")
    raise RuntimeError(
        "Définir GOOGLE_SA_KEY_JSON ou GOOGLE_SA_KEY_PATH (fichier JSON GCP)"
    )


def env(key: str, default: Optional[str] = None) -> str:
    raw = os.getenv(key, default)
    if raw is None:
        raise RuntimeError(f"Missing required environment variable: {key}")
    val = raw.strip().strip('"').strip("'")
    if not val:
        raise RuntimeError(f"Environment variable empty: {key}")
    return val

def build_pg_url(
    user: str,
    password: str,
    host: str,
    port: str,
    db: str,
    sslmode: Optional[str] = None,
) -> str:
    url = (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(db, safe='')}"
    )
    if sslmode:
        url += f"?sslmode={sslmode}"
    return url

# ─── Google Sheets ────────────────────────────────────────────────────────────
def read_worksheet(sheet_id: str, worksheet: str, sa_json: str) -> pd.DataFrame:
    """Return the worksheet content as a DataFrame."""
    logger.info("Authenticating to Google Sheets using inline service account JSON.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)

    logger.info("Opening spreadsheet %s (worksheet=%s).", sheet_id, worksheet)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet)

    logger.info("Fetching records from worksheet…")
    records = ws.get_all_records()  # first row = headers
    df = pd.DataFrame(records)

    if df.empty:
        logger.warning("Worksheet is empty. Nothing to load.")
    else:
        logger.info("Pulled %d rows × %d columns.", df.shape[0], df.shape[1])

    return df

# ─── Database load ────────────────────────────────────────────────────────────
def get_engine(pg_url: str) -> Engine:
    logger.info("Creating SQLAlchemy engine.")
    return create_engine(pg_url)

def load_dataframe(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    if_exists: str = "replace",
    chunksize: Optional[int] = 1000,
) -> None:
    """Write the DataFrame to PostgreSQL."""
    if df.empty:
        logger.info("Skip loading: DataFrame is empty.")
        return

    logger.info(
        "Loading DataFrame into table '%s' (if_exists=%s, chunksize=%s)…",
        table, if_exists, chunksize,
    )
    df.to_sql(
        table,
        engine,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method="multi",
    )
    logger.info("Load completed.")

# ─── Main orchestrator ────────────────────────────────────────────────────────
def main() -> None:
    load_local_env()
    sheet_id = env("SHEET_ID")
    worksheet = env("WORKSHEET")
    sa_json = resolve_google_sa_json()

    pg_user = env("PG_USER")
    pg_password = env("PG_PASSWORD")
    pg_host = env("PG_HOST")
    pg_port = env("PG_PORT")
    pg_db = env("PG_DB")
    pg_table = env("PG_TABLE")
    pg_if_exists = "replace"
    pg_chunksize_str = "1000"
    pg_sslmode = os.getenv("PG_SSLMODE", "").strip() or None
    if pg_sslmode is None and "supabase.co" in pg_host:
        pg_sslmode = "require"

    try:
        pg_chunksize = int(pg_chunksize_str) if pg_chunksize_str else None
    except ValueError:
        raise RuntimeError("PG_CHUNKSIZE must be an integer or empty.")

    # Extract
    df = read_worksheet(sheet_id, worksheet, sa_json)

    # Load
    pg_url = build_pg_url(
        pg_user, pg_password, pg_host, pg_port, pg_db, sslmode=pg_sslmode
    )
    engine = get_engine(pg_url)
    load_dataframe(df, engine, pg_table, if_exists=pg_if_exists, chunksize=pg_chunksize)

if __name__ == "__main__":
    main()
