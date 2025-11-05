import time
from datetime import datetime
import hashlib
import json
import logging
from kafka_pipeline.logs.config_logs import setup_logging
from kafka import KafkaProducer, KafkaConsumer
setup_logging()

SOURCE_BOOTSTRAP = "46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294"
SOURCE_USERNAME = "kafka"
SOURCE_PASSWORD = "UnigapKafka@2024"
SOURCE_TOPIC = "product_view"

LOCAL_BOOTSTRAP = "localhost:9094,localhost:9194,localhost:9294"
LOCAL_USERNAME = "admin"
LOCAL_PASSWORD = "17022003"
LOCAL_TOPICS = ["part-1", "part-2", "part-3", "part-4"]

TARGET_COUNT = 10000

def get_target_topic(record):
    key = str(record.get('id', record.get('user_id', 'unknown')))
    idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % 4
    return LOCAL_TOPICS[idx]

def main():
    logging.info("Starting test")

    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=SOURCE_BOOTSTRAP.split(','),
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=SOURCE_USERNAME,
        sasl_plain_password=SOURCE_PASSWORD,
        auto_offset_reset='earliest',
        #group_id='test-transfer-group',
        group_id=f'test-transfer-{int(time.time())}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=300000  # Dừng nếu không có tin mới
    )

    producer = KafkaProducer(
        bootstrap_servers=LOCAL_BOOTSTRAP.split(','),
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=LOCAL_USERNAME,
        sasl_plain_password=LOCAL_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    count = 0
    counts_per_topic = {t: 0 for t in LOCAL_TOPICS}

    try:
        for message in consumer:
            if count >= TARGET_COUNT:
                break

            data = message.value
            data['test_transferred_at'] = datetime.now().isoformat()
            data['test_source_offset'] = message.offset

            target_topic = get_target_topic(data)
            producer.send(target_topic, value=data)
            counts_per_topic[target_topic] += 1
            count += 1

            if count % 1000 == 0:
                logging.info(f"Đã chuyển {count} bản ghi | {dict(counts_per_topic)}")

        producer.flush()
        logging.info(f"HOÀN TẤT! Đã chuyển {count} bản ghi vào Kafka bạn:")
        for t, c in counts_per_topic.items():
            logging.info(f"  → {t}: {c} bản ghi")

    except Exception as e:
        logging.error(f"Lỗi: {e}")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()


