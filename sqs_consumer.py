from dotenv import load_dotenv
import boto3
import json
import time
import os

from spark_writer import spark_insert


load_dotenv()

region = os.getenv("AWS_DEFAULT_REGION")
sqs_url = os.getenv("SPARK_INSERT_SQS_URL")

sqs = boto3.client("sqs", region_name=region)

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
            time.sleep(3)
            continue

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            try:
                body = json.loads(msg["Body"])
                print(f"Received: {body}")

                # spark_insert 함수 호출 (직접 context로 전달)
                spark_insert(body)

                # 메시지 정상 처리시 SQS에서 삭제
                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=receipt_handle)
                print("Message deleted.")

            except Exception as e:
                print(f"Error handling message: {e}")

if __name__ == "__main__":
    consume_sqs()