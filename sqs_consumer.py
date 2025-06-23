from collections import defaultdict
from dotenv import load_dotenv

from sensor_schema import SensorType
from spark_writer import get_spark_session, batch_insert

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

def delete_messages_batch(sqs, sqs_url, batch):
    entries = [
        {"Id": str(idx), "ReceiptHandle": msg["ReceiptHandle"]}
        for idx, (msg, _) in enumerate(batch)
    ]
    if entries:
        sqs.delete_message_batch(QueueUrl=sqs_url, Entries=entries)

def process_sensor_type_batch(sensor_type, batch, spark_session, sqs, sqs_url):
    data_list = []
    for _, body in batch:
        if isinstance(body["data"], list):
            data_list.extend(body["data"])
        else:
            data_list.append(body["data"])

    batch_insert(sensor_type, data_list, spark_session)
    delete_messages_batch(sqs, sqs_url, batch)

def consume_sqs():
    while True:
        resp = sqs.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
        )
        messages = resp.get("Messages", [])

        if not messages:
            logger.info("No message. Wait...")
            time.sleep(5)
            continue

        data_dict = group_by_sensor_type(messages)

        for sensor_type, batch in data_dict.items():
            try:
                process_sensor_type_batch(sensor_type, batch, spark_session, sqs, sqs_url)
            except Exception as e:
                logger.error(f"Batch insert or delete failed for {sensor_type}: {e}")

if __name__ == "__main__":
    try:
        consume_sqs()
    finally:
        spark_session.stop()