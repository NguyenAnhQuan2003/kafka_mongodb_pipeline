from typing import List

from pydantic import BaseSettings

class Settings(BaseSettings):
    SOURCE_BOOTSTRAP: str = "46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294"
    SOURCE_USERNAME: str = "kafka_pipeline"
    SOURCE_PASSWORD: str = "UnigapKafka@2024"
    SOURCE_TOPIC: str = "product_view"

    LOCAL_BOOTSTRAP: str = "localhost:9094,localhost:9194,localhost:9294"
    LOCAL_USERNAME: str = "admin"
    LOCAL_PASSWORD: str = "17022003"
    LOCAL_TOPICS: List[str]

    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "kafka_lab"
    MONGO_COLLECTION: str = "product_views"

    BATCH_SIZE: int = 1000

    class Config:
        env_file = ".env"

settings = Settings()