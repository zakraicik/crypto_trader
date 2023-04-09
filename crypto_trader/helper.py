import boto3
import io
import logging
from datetime import datetime

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


def compute_number_data_points(start_date, end_date, interval):
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    date_difference = (end_date - start_date).days

    number_of_data_points = (INTERVAL_MAPPING[interval] * date_difference) + 1

    return number_of_data_points


def df_to_s3(df, bucket_name, file_key, aws_access_key_id, aws_secret_access_key):
    """
    Saves a pandas dataframe to S3 as a JSON file.

    Args:
        df (pandas.DataFrame): The dataframe to save.
        bucket_name (str): The name of the S3 bucket to save the file in.
        file_key (str): The key (i.e. filename) to save the file under in the S3 bucket.
    """

    # Convert the dataframe to a JSON string
    json_string = df.to_json(orient="records")

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
