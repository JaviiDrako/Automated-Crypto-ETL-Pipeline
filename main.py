import time
import logging
import mysql.connector
from etl.extract import (
    get_coins_and_snapshots,
    get_exchanges,
    get_coin_ohlc,
    get_market_history
)
from etl.transform import (
    transform_coins_and_snapshots,
    transform_exchanges,
    transform_coin_ohlc,
    transform_market_history
)
from etl.load import (
    load_coins_and_snapshots,
    load_exchanges,
    load_coin_ohlc,
    load_market_history
)
from dotenv import load_dotenv
import os

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load variables from .env
load_dotenv()

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=os.getenv("MYSQL_PORT")
    )

def has_market_history_bootstrap(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM market_history")
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

def has_ohlc_botstrap(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM coin_ohlc")
    count = cursor.fetchone()[0]
    return count > 0


def main():
    logging.info("🚀 Starting ETL pipeline...")

    conn = get_db_connection()

    # Crypto coins list
    coins = [
        "bitcoin",
        "ethereum",
        "binancecoin",
        "cardano",
        "solana",
        "ripple",
        "dogecoin",
        "chainlink",
        "litecoin",
        "terra-luna",
        # Stablecoins
        "tether",
        "usd-coin",
        "binance-usd",
        "dai",
        "true-usd",
        "gemini-dollar"
    ]

    try:
        # =====================
        # 1. Coins + Snapshots
        # =====================
        logging.info("📥 Extracting coins + snapshots...")
        coins_raw = get_coins_and_snapshots(coins)
        coins_df, snapshots_df = transform_coins_and_snapshots(coins_raw)
        load_coins_and_snapshots(coins_df, snapshots_df, conn)

        # =====================
        # 2. Exchanges
        # =====================
        logging.info("📥 Extracting exchanges...")
        exchanges_raw = get_exchanges()
        exchanges_df = transform_exchanges(exchanges_raw)
        load_exchanges(exchanges_df, conn)

        # =====================
        # 3. Coin OHLC
        # =====================
        bootstrap = not has_ohlc_botstrap(conn)

        for coin in coins:
            logging.info(f"📥 Extracting OHLC of {coin}...")

            days = 30 if bootstrap else 1
            ohlc_raw = get_coin_ohlc(coin, vs_currency="usd", days=days)
            if ohlc_raw:
                ohlc_df = transform_coin_ohlc(ohlc_raw, coin)
                load_coin_ohlc(ohlc_df, conn)
            time.sleep(30)

        # =====================
        # 4. Market History (only once)
        # =====================
        if not has_market_history_bootstrap(conn):
            for coin in coins:
                logging.info(f"📥 Extracting market history of {coin}...")
                history_raw = get_market_history(coin, vs_currency="usd", days="100")
                if history_raw:
                    history_df = transform_market_history(history_raw, coin)
                    load_market_history(history_df, conn)
                time.sleep(30)
        else:
            logging.info("[SKIP] market_history already contains data. It will not be reloaded")


        logging.info("✅ ETL completed successfully.")

    except Exception as e:
        logging.error(f"❌ Error in the ETL pipeline: {e}")

    finally:
        conn.close()
        logging.info("🔌 Closed connection.")


if __name__ == "__main__":
    main()
