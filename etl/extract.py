import requests
import logging

BASE_URL = "https://api.coingecko.com/api/v3"


def _make_request(endpoint, params=None):
    """Helper for requests with error handling."""
    url = f"{BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            logging.warning(f"[429] Rate limited in {url}")
            return None
        else:
            logging.error(f"[{resp.status_code}] Error in {url}: {resp.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[EXCEPTION] {url} -> {e}")
        return None


# -----------------------
# 1. COINS + SNAPSHOTS
# -----------------------
def get_coins_and_snapshots(coin_ids, vs_currency="usd"):
    """
    /coins/markets -> static info (coins) + dynamic (snapshots)
    """
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coin_ids),
    }
    return _make_request("/coins/markets", params=params)


# -----------------------
# 2. MARKET HISTORY
# -----------------------
def get_market_history(coin_id, vs_currency="usd", days="180"):
    """
    /coins/{id}/market_chart -> historical series (price, market_cap, volume)
    """
    params = {"vs_currency": vs_currency, "days": days}
    return _make_request(f"/coins/{coin_id}/market_chart", params=params)


# -----------------------
# 3. EXCHANGES
# -----------------------
def get_exchanges(per_page=50, page=1):
    """
    /exchanges -> exxchanges list
    """
    params = {"per_page": per_page, "page": page}
    return _make_request("/exchanges", params=params)


def get_exchange_details(exchange_id):
    """
    /exchanges/{id} -> specific details of an exchange
    """
    return _make_request(f"/exchanges/{exchange_id}")


# -----------------------
# 4. OHLC
# -----------------------
def get_coin_ohlc(coin_id, vs_currency="usd", days=30):
    """
    /coins/{id}/ohlc -> candlesticks OHLC
    """
    params = {"vs_currency": vs_currency, "days": days}
    return _make_request(f"/coins/{coin_id}/ohlc", params=params)
