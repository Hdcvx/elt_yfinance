import os

import pandas as pd
import psycopg2
import yfinance as yf
from loguru import logger
from psycopg2.extras import execute_values

TICKERS = [
    # Petróleo & Energia
    "PETR4.SA", "PRIO3.SA",
    # Bancos
    "ITUB4.SA", "BBAS3.SA", "BBDC4.SA", "SANB11.SA",
    # Commodities
    "VALE3.SA", "GGBR4.SA", "SUZB3.SA",
    # Consumo & Varejo
    "ABEV3.SA", "LREN3.SA", "MGLU3.SA",
    # Industrial
    "WEGE3.SA", "RENT3.SA",
    # Saúde
    "HAPV3.SA", "RADL3.SA",
]

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres-dados"),
    "port": 5432,
    "dbname": "data_warehouse",
    "user": "dados_user",
    "password": "dados_pass",
}


def extract_load(tickers: list[str], days: int = 365):
    logger.info(f"Baixando {len(tickers)} ativos — últimos {days} dias")

    df = yf.download(tickers, period=f"{days}d", auto_adjust=True, progress=False)
    df = (
        df.stack(level="Ticker", future_stack=True)
        .reset_index()
        .rename(columns={"Ticker": "ticker"})
    )
    df.columns.name = None

    pg = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = pg.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.precos (
            date     DATE             NOT NULL,
            ticker   VARCHAR          NOT NULL,
            open     DOUBLE PRECISION,
            close    DOUBLE PRECISION,
            high     DOUBLE PRECISION,
            low      DOUBLE PRECISION,
            volume   BIGINT,
            PRIMARY KEY (ticker, date)
        )
    """)

    records = [
        (
            row.Date.date() if hasattr(row.Date, "date") else row.Date,
            row.ticker,
            None if pd.isna(row.Open) else float(row.Open),
            None if pd.isna(row.Close) else float(row.Close),
            None if pd.isna(row.High) else float(row.High),
            None if pd.isna(row.Low) else float(row.Low),
            None if pd.isna(row.Volume) else int(row.Volume),
        )
        for row in df.itertuples()
    ]

    execute_values(
        cursor,
        """
        INSERT INTO bronze.precos (date, ticker, open, close, high, low, volume)
        VALUES %s
        ON CONFLICT (ticker, date) DO UPDATE SET
            open   = EXCLUDED.open,
            close  = EXCLUDED.close,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            volume = EXCLUDED.volume
        """,
        records,
    )

    pg.commit()
    logger.success(f"{len(records)} registros upsertados em bronze.precos")
    cursor.close()
    pg.close()


if __name__ == "__main__":
    extract_load(TICKERS, days=365)
