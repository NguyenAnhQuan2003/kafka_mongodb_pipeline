import json
import sys
from pathlib import Path
from pymongo import UpdateOne
from kafka_pipeline.config.address import INPUT_FILE_DB, MONGO_URI, MONGO_DB, MONGO_COLLECTION_T
from kafka_pipeline.config.mongodb_connect import MongoConfig, get_mongo_client, get_collection_name, get_db
import logging
import os
from kafka_pipeline.logs.config_logs import setup_logging

setup_logging()

INPUT_FILE = os.path.join(INPUT_FILE_DB, "product_views.jsonl")

def import_mongodb():
    file_path = Path(INPUT_FILE)
    if not file_path.exists():
        logging.error(f"Không tìm thấy file: {file_path}")
        sys.exit(1)

    logging.info(f"Bắt đầu import từ: {file_path}")
    cfg = MongoConfig(
        uri=MONGO_URI,
        db_name=MONGO_DB
    )
    client = get_mongo_client(cfg)
    collection = get_collection_name(client, cfg.db_name, MONGO_COLLECTION_T)
    logging.info(f"Connected to mongodb : {collection}")

    total = 0
    batch = []
    BATCH_SIZE = 1000  # Tối ưu tốc độ

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    doc = json.loads(line)
                    doc_id = doc.get("id")
                    if not doc_id:
                        logging.warning(f"Dòng {line_num}: Không có 'id' → Bỏ qua")
                        continue
                    doc["_id"] = doc_id  # Dùng 'id' làm '_id' để upsert
                    batch.append(doc)
                except json.JSONDecodeError as e:
                    logging.warning(f"Dòng {line_num}: JSON lỗi → {e}")
                    continue

                # Ghi batch
                if len(batch) >= BATCH_SIZE:
                    operations = [
                        UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True)
                        for d in batch
                    ]
                    result = collection.bulk_write(operations, ordered=False)
                    inserted = result.upserted_count + result.modified_count
                    total += inserted
                    logging.info(f"Đã import {total} bản ghi...")
                    batch.clear()

        # Ghi nốt phần còn lại
        if batch:
            operations = [
                {
                    "update_one": {
                        "filter": {"_id": d["_id"]},
                        "update": {"$set": d},
                        "upsert": True
                    }
                }
                for d in batch
            ]
            result = collection.bulk_write(operations, ordered=False)
            total += result.upserted_count + result.modified_count

        logging.info(f"HOÀN TẤT IMPORT! → {total} bản ghi vào {MONGO_DB}.{MONGO_COLLECTION_T}")

    except Exception as e:
        logging.error(f"LỖI KHI IMPORT: {e}")
        sys.exit(1)
    finally:
        client.close()
        logging.info("Đã đóng kết nối MongoDB.")
if __name__ == "__main__":
    import_mongodb()