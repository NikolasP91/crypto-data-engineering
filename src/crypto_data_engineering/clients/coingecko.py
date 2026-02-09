import requests


COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


def fetch_markets(vs_currency: str = "usd", per_page: int = 50) -> list[dict]:
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": False,
    }
    response = requests.get(COINGECKO_MARKETS_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()
