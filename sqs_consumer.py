from dotenv import load_dotenv
import boto3
import json
import time
import os
import logging

from spark_writer import  get_spark_session, spark_insert

load_dotenv()
region = os.getenv("AWS_DEFAULT_REGION")
sqs_url = os.getenv("SPARK_INSERT_SQS_URL")
sqs = boto3.client("sqs", region_name=region)

spark_session = get_spark_session()
logger = logging.getLogger(__name__)

def handle_message(msg):
    receipt_handle = msg["ReceiptHandle"]
    try:
        body = json.loads(msg["Body"])
        spark_insert(body, spark_session)

        sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=receipt_handle)
        print("[DEBUG] SQS 메시지 삭제 완료")
    except Exception as e:
        print(f"Error handling message: {e}")
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