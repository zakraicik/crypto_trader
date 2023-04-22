import boto3
import logging
import json
import requests
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt


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


def from_s3(
    bucket: str, key: str, access_key_id: str, secret_access_key: str
) -> pd.DataFrame:
    s3 = boto3.client(
        "s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")
    df = pd.read_json(data, orient="records")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    return df


def plot_signals(data: pd.DataFrame, signals: pd.DataFrame):
    # Merge price data and signals
    data_signals = pd.merge(
        data, signals, left_on="open_time", right_on="timestamp", how="left"
    )

    # Plot the close price
    plt.figure(figsize=(14, 8))
    plt.plot(
        data_signals["timestamp"], data_signals["close"], label="Price", linewidth=1
    )

    # Plot short SMA
    plt.plot(
        data_signals["timestamp"],
        data_signals["short_sma"],
        label=f"Short SMA",
        linewidth=1,
    )

    # Plot long SMA
    plt.plot(
        data_signals["timestamp"],
        data_signals["long_sma"],
        label=f"Long SMA",
        linewidth=1,
    )

    # Overlay buy signals
    buy_signals = data_signals[data_signals["position"] == 1]
    plt.scatter(
        buy_signals["timestamp"],
        buy_signals["close"],
        label="Buy",
        marker="^",
        color="g",
    )

    # Overlay sell signals
    sell_signals = data_signals[data_signals["position"] == -1]
    plt.scatter(
        sell_signals["timestamp"],
        sell_signals["close"],
        label="Sell",
        marker="v",
        color="r",
    )

    # Add labels and legend
    plt.xlabel("Timestamp")
    plt.ylabel("Price")
    plt.legend(loc="best")
    plt.title("Price data with Buy/Sell signals and SMAs")

    # Display the plot
    plt.show()
