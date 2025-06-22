from collections import defaultdict
from dotenv import load_dotenv

from sensor_schema import SensorType
from spark_writer import get_spark_session, spark_insert

import boto3
import json
import time
import os
import logging

load_dotenv()
region = os.getenv("AWS_DEFAULT_REGION")
sqs_url = os.getenv("SPARK_INSERT_SQS_URL")
sqs = boto3.client("sqs", region_name=region)

spark_session = get_spark_session()
logger = logging.getLogger(__name__)

def group_by_sensor_type(messages):
    grouped = defaultdict(list)
    for msg in messages:
        body = json.loads(msg["Body"])
        try:
            sensor_type = SensorType(body["sensor_type"])
        except ValueError:
            logger.error(f"Unknown sensor_type: {body.get('sensor_type')}")
            continue
        grouped[sensor_type].append((msg, body))
    return grouped

def handle_message(msg):
    receipt_handle = msg["ReceiptHandle"]
    try:
        body = json.loads(msg["Body"])
        spark_insert(body, spark_session)

        sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=receipt_handle)
    except Exception as e:
        logger.error(f"Error handling message: {e}", exc_info=True)

def consume_sqs():
    while True:
        resp = sqs.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
        )
        messages = resp.get("Messages", [])

        if not messages:
            print("No message. Wait...")
            time.sleep(5)
            continue

        for msg in messages:
            handle_message(msg)

if __name__ == "__main__":
    try:
        consume_sqs()
    finally:
        spark_session.stop()