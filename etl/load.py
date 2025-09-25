import logging
import pandas as pd
from mysql.connector import Error

def insert_dataframe(df: pd.DataFrame, table: str, conn, update=False):
    """
    Inserts a DataFrame into MySQL.
    - df: clean DataFrame (transform.py)
    - table: target table name
    - conn: active MySQL connection
    - update: if True, UPSERT (update on duplicate key)
    """
    if df is None or df.empty:
        logging.warning(f"[SKIP] No data to insert into {table}")
        return

    cursor = conn.cursor()

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    # Base query
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    # parameterized UPSERT
    if update:
        updates = ", ".join([f"{col}=VALUES({col})" for col in df.columns if col not in ["coin_id", "exchange_id"]])
        sql += f" ON DUPLICATE KEY UPDATE {updates}"

    try:
        cursor.executemany(sql, df.values.tolist())
        conn.commit()
        logging.info(f"[OK] Inserted {cursor.rowcount} rows into {table}")
    except Error as e:
        logging.error(f"[DB INSERT ERROR] {e} -> {table}")
        conn.rollback()
    finally:
        cursor.close()


# -----------------------
# Specific load functions
# -----------------------

def load_coins_and_snapshots(coins_df, snapshots_df, conn):
    insert_dataframe(coins_df, "coins", conn, update=True)
    insert_dataframe(snapshots_df, "market_snapshots", conn, update=False)

def load_market_history(history_df, conn):
    insert_dataframe(history_df, "market_history", conn, update=False)

def load_exchanges(exchanges_df, conn):
    insert_dataframe(exchanges_df, "exchanges", conn, update=True)

def load_coin_ohlc(ohlc_df, conn):
    insert_dataframe(ohlc_df, "coin_ohlc", conn, update=False)

