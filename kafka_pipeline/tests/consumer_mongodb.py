import json
import logging
import os
from kafka_pipeline.logs.config_logs import setup_logging
from dotenv import load_dotenv
from kafka_pipeline.config.address import OUTPUT_DIR
from kafka import KafkaConsumer
load_dotenv()
setup_logging()

LOCAL_BOOTSTRAP = os.getenv("LOCAL_BOOTSTRAP")
LOCAL_USERNAME = os.getenv("LOCAL_USERNAME")
LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
LOCAL_TOPICS = os.getenv("LOCAL_TOPICS").split(',')
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "product_views.jsonl")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def export_from_kafka():
    logging.info(f"Bắt đầu xuất dữ liệu từ {len(LOCAL_TOPICS)} topic → {OUTPUT_FILE}")

    consumer = KafkaConsumer(
        *LOCAL_TOPICS,
        bootstrap_servers=LOCAL_BOOTSTRAP.split(','),
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=LOCAL_USERNAME,
        sasl_plain_password=LOCAL_PASSWORD,
        auto_offset_reset='earliest',
        group_id=f'export-group-{int(__import__("time").time())}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=300000  # 5 phút
    )

    total = 0
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        batch = []
        for message in consumer:
            data = message.value.copy()
            data.pop('test_transferred_at', None)
            data.pop('test_source_offset', None)
            batch.append(data)

            if len(batch) >= BATCH_SIZE:
                for item in batch:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
                total += len(batch)
                logging.info(f"Đã xuất {total} bản ghi...")
                batch.clear()

        if batch:
            for item in batch:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
            total += len(batch)

    consumer.close()
    logging.info(f"HOÀN TẤT XUẤT! → {total} bản ghi → {OUTPUT_FILE}")
    return total

if __name__ == "__main__":
    export_from_kafka()