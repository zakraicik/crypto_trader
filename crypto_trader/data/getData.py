import argparse
import datetime as dt
import logging
import datetime

from typing import Optional, List

from crypto_trader.helper import (
    to_s3,
    compute_number_data_points,
    make_api_request,
    convert_to_json,
    MAX_DATA_POINTS,
    URL,
    INTERVAL_MAPPING,
    BUCKET,
)
from crypto_trader.keys import (
    API_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
)

logger = logging.getLogger(__name__)


def get_historical_prices(
    symbol: str,
    interval: str,
    start_date: dt.datetime,
    end_date: dt.datetime,
) -> Optional[List[dict]]:
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(dt.datetime.timestamp(start_date) * 1000),
        "endTime": int(dt.datetime.timestamp(end_date) * 1000),
        "limit": 1000,
    }

    headers = {"X-MBX-APIKEY": API_KEY}

    number_data_points = compute_number_data_points(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), interval
    )

    if number_data_points > MAX_DATA_POINTS:
        response_list = []

        number_requests = int(number_data_points // 1000) + 1

        data_points_per_day = INTERVAL_MAPPING[interval]
        number_of_days_per_request = 1000 / data_points_per_day

        request_start_date = start_date
        request_end_date = start_date + datetime.timedelta(
            days=number_of_days_per_request
        )
        for i in range(number_requests):
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": int(dt.datetime.timestamp(request_start_date) * 1000),
                "endTime": int(dt.datetime.timestamp(request_end_date) * 1000),
                "limit": 1000,
            }

            response = make_api_request(URL, params, headers)

            if response is not None:
                json_data = convert_to_json(response.json())
                response_list = response_list + json_data

            request_start_date = request_end_date + datetime.timedelta(days=1)

            request_end_date = min(
                request_start_date
                + datetime.timedelta(days=number_of_days_per_request),
                datetime.datetime.combine(datetime.datetime.today(), datetime.time.min),
            )

        return response_list

    else:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(dt.datetime.timestamp(start_date) * 1000),
            "endTime": int(dt.datetime.timestamp(end_date) * 1000),
            "limit": 1000,
        }

        response = make_api_request(URL, params, headers)

        if response is not None:
            json_data = convert_to_json(response.json())
            return json_data


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

    response = get_historical_prices(
        args.symbol, args.interval, args.start_date, args.end_date
    )

    s3_path = f"data/{args.symbol}/{args.start_date.strftime('%Y_%m_%d')}_{args.end_date.strftime('%Y_%m_%d')}_{args.interval}.json"

    to_s3(
        response,
        BUCKET,
        s3_path,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
    )

    logger.info("Data saved to S3 successfully")


if __name__ == "__main__":
    main()
