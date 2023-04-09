import boto3
import io
import logging


logger = logging.getLogger(__name__)


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
        logger.info(f"Dataframe saved to s3://{bucket_name}/{file_key}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving dataframe to S3: {e}")
        
        return False
