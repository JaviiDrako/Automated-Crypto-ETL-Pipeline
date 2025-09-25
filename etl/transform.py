import pandas as pd
import numpy as np

# -----------------------
# 1. Coins + Snapshots
# -----------------------
def transform_coins_and_snapshots(raw_json):
    """
    Transform the JSON from /coins/markets into two DataFrames:
    - coins_df: static info
    - snapshots_df: dynamic info
    """
    if raw_json is None:
        return None, None
    
    df = pd.DataFrame(raw_json)

    # --- Coins ---
    coins_df = df[[
        "id", "symbol", "name", "image", "market_cap_rank",
        "high_24h", "low_24h", "ath", "ath_date",
        "atl", "atl_date", "max_supply"
    ]].copy()

    coins_df.rename(columns={
        "id": "coin_id",
        "image": "image_url"
    }, inplace=True)

    # Convert date columns
    coins_df["ath_date"] = pd.to_datetime(coins_df["ath_date"], errors="coerce")
    coins_df["atl_date"] = pd.to_datetime(coins_df["atl_date"], errors="coerce")

    # Limit decimal fields
    decimals_dict = {
        "high_24h": 5,
        "low_24h": 5,
        "ath": 5,
        "atl": 5,
    }

    coins_df = coins_df.round(decimals_dict)


    # --- Market Snapshots ---
    snapshots_df = df[[
        "id", "current_price", "market_cap", "circulating_supply",
        "total_volume", "price_change_percentage_24h",
        "market_cap_change_percentage_24h", "ath_change_percentage",
        "atl_change_percentage", "last_updated"
    ]].copy()

    snapshots_df.rename(columns={"id": "coin_id"}, inplace=True)
    snapshots_df["last_updated"] = pd.to_datetime(snapshots_df["last_updated"], errors="coerce")
    decimals_dict = {
        "current_price": 5,
        "market_cap": 2,
        "total_volume": 2,
        "price_change_percentage_24h": 5,
        "market_cap_change_percentage_24h": 5,
        "ath_change_percentage": 5,
        "atl_change_percentage": 5,
    }
    snapshots_df = snapshots_df.round(decimals_dict)

    # Replace NaNs with None
    coins_df = coins_df.replace({np.nan: None})
    snapshots_df = snapshots_df.replace({np.nan: None})

    return coins_df, snapshots_df


# -----------------------
# 2. Market History
# -----------------------
def transform_market_history(raw_json, coin_id):
    """
    Transform the JSON from /coins/{id}/market_chart into a DataFrame:
    """
    if raw_json is None:
        return None
    
    prices = raw_json.get("prices", [])
    market_caps = raw_json.get("market_caps", [])
    volumes = raw_json.get("total_volumes", [])

    df_prices = pd.DataFrame(prices, columns=["timestamp", "price"])
    df_mc = pd.DataFrame(market_caps, columns=["timestamp", "market_cap"])
    df_vol = pd.DataFrame(volumes, columns=["timestamp", "volume"])

    # Unir todo por timestamp
    df = df_prices.merge(df_mc, on="timestamp").merge(df_vol, on="timestamp")

    # Milisegundos → fecha
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    df["coin_id"] = coin_id

    return df[["coin_id", "timestamp", "price", "market_cap", "volume"]]


# -----------------------
# 3. Exchanges
# -----------------------
def transform_exchanges(raw_json):
    """
    Transform JSON /exchanges into DataFrame
    """
    if raw_json is None:
        return None
    
    df = pd.DataFrame(raw_json)

    df = df[[
        "id", "name", "year_established", "country",
        "url", "image", "trust_score", "trust_score_rank",
        "trade_volume_24h_btc"
    ]].copy()

    df.rename(columns={
        "id": "exchange_id",
        "image": "image_url"
    }, inplace=True)

    return df.replace({np.nan: None})


# -----------------------
# 4. OHLC
# -----------------------
def transform_coin_ohlc(raw_json, coin_id):
    """
    Transform JSON /coins/{id}/ohlc into DataFrame
    """
    if raw_json is None:
        return None
    
    df = pd.DataFrame(raw_json, columns=[
        "timestamp", "open_price", "high_price", "low_price", "close_price"
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["coin_id"] = coin_id

    return df[["coin_id", "timestamp", "open_price", "high_price", "low_price", "close_price"]]

