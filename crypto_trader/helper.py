import boto3
import io
import logging
import json
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

INTERVAL_MAPPING = {
    "1m": 1440,
    "3m": 480,
    "5m": 288,
    "15m": 96,
    "30m": 48,
    "1h": 24,
    "2h": 12,
    "4h": 6,
    "6h": 4,
    "8h": 3,
    "12h": 2,
    "1d": 1,
}

BUCKET = "cyrpto-trading-bot"

MAX_DATA_POINTS = 1000

URL = "https://api.binance.us/api/v3/klines"


def compute_number_data_points(start_date, end_date, interval):
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    date_difference = (end_date - start_date).days

    number_of_data_points = (INTERVAL_MAPPING[interval] * date_difference) + 1

    return number_of_data_points


def convert_to_json(kline_data):
    keys = [
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
    ]

    json_data = [dict(zip(keys, candlestick)) for candlestick in kline_data]

    return json_data


def make_api_request(url, params, headers):
    try:
        response = requests.get(url, params=params, headers=headers)

        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")

    return response


def to_s3(response, bucket_name, file_key, aws_access_key_id, aws_secret_access_key):
    # Convert the dataframe to a JSON string
    json_string = json.dumps(response)

    # Create an S3 client
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Save the JSON string to S3
    try:
        response = s3.put_object(Bucket=bucket_name, Key=file_key, Body=json_string)
        return True

    except Exception as e:
        return False


def from_s3(response, bucket_name, file_key, aws_access_key_id, aws_secret_access_key):
    pass
