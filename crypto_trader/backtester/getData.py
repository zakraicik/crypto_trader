import argparse
import datetime as dt
import logging
import pandas as pd
import requests

from typing import Optional

from crypto_trader.helper import df_to_s3
from crypto_trader.config import (
    API_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BUCKET,
)

logger = logging.getLogger(__name__)


def get_historical_prices(
    symbol: str,
    interval: str,
    start_date: dt.datetime,
    end_date: dt.datetime,
) -> Optional[pd.DataFrame]:
    """
    Retrieves historical price data for a given cryptocurrency symbol from the Binance API.

    Args:
        symbol (str): The symbol of the cryptocurrency to retrieve price data for (e.g. 'ETHUSDT').
        interval (str): The interval at which to retrieve price data (e.g. '1d' for daily data).
        start_date (datetime): The start date for the price data to retrieve.
        end_date (datetime): The end date for the price data to retrieve.
        api_key (str): Your Binance API key.

    Returns:
        pandas.DataFrame: A DataFrame containing the historical price data, with one row per interval.

    Raises:
        requests.exceptions.RequestException: If an error occurs while making the API request.
        ValueError: If an error occurs while converting the API response to a DataFrame.

    """

    url = "https://api.binance.us/api/v3/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(dt.datetime.timestamp(start_date) * 1000),
        "endTime": int(dt.datetime.timestamp(end_date) * 1000),
        "limit": 1000,
    }

    headers = {"X-MBX-APIKEY": API_KEY}

    try:
        response = requests.get(url, params=params, headers=headers)

        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")

        return None

    try:
        df = pd.DataFrame(
            response.json(),
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )

        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")

        df.set_index("open_time", inplace=True)

        df = df.astype(float)

        return df

    except ValueError as e:
        logger.error(f"Error converting response to dataframe: {e}")

        return None


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default="ETHUSDT",
        help="The symbol of the cryptocurrency to retrieve price data for (e.g. 'ETHUSDT')",
    )

    parser.add_argument(
        "--interval",
        default="1d",
        help="The interval at which to retrieve price data (e.g. '1d' for daily data)",
    )
    parser.add_argument(
        "--start_date",
        type=dt.datetime.fromisoformat,
        help="The start date for the price data to retrieve, in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=dt.datetime.fromisoformat,
        help="The end date for the price data to retrieve, in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    df = get_historical_prices(
        args.symbol, args.interval, args.start_date, args.end_date
    )

    s3_path = f"data/{args.symbol}_{args.start_date.strftime('%Y_%m_%d')}_{args.end_date.strftime('%Y_%m_%d')}"

    df_to_s3(
        df,
        BUCKET,
        s3_path,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
    )

    logger.info("Data saved to S3 successfully")


if __name__ == "__main__":
    main()
