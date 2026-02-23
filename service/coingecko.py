import time
from urllib.error import HTTPError

import requests

from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

logger = get_logger("Coingecko API")


BASE_URL = "https://api.coingecko.com/api/v3"
SLEEP = 80


def _format_prices(prices: list[list[float]]) -> dict[int, float]:
    formatted_prices: dict[int, float] = {}
    _ = prices.pop()  # remove the latest price
    for price in prices:
        timestamp = int(price[0] / 1e3)
        price_value = price[1]
        formatted_prices[timestamp] = price_value
    return formatted_prices


@retry_handler(retries_number=10, sleep_time=SLEEP)
def get_historical_prices(
    coin_id: str | None, days: int = 90, vs_currency: str = "usd"
) -> dict[int, float]:
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    response = requests.get(url, params=params)
    time.sleep(5)
    data = response.json()
    if response.status_code == 200:
        prices = data.get("prices", [])
        formatted_prices = _format_prices(prices)
        return formatted_prices
    elif response.status_code == 404 and data.get("error") == "coin not found":
        logger.info(f"Coin not found: {coin_id}")
        return {}
    elif response.status_code == 429:
        raise HTTPError(
            url,
            response.status_code,
            f"Too many requests: token {coin_id} . Sleep {SLEEP}s",
            {},  # pyright: ignore [reportArgumentType]
            None,
        )
    else:
        raise HTTPError(url, response.status_code, msg=data, fp=None, hdrs=None)  # pyright: ignore [reportArgumentType]


if __name__ == "__main__":
    _token_id = "0chain_"
    hist = get_historical_prices(coin_id=_token_id)
    print(hist)
