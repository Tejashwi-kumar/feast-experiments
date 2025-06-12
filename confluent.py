# In a real project, this would be organized into separate files.
# For this example, I'm combining them with clear separators.
#
# --- PROJECT STRUCTURE (REFACTORED) ---
#
# /your_project
# |
# |- main.py                 # FastAPI app with consumer factory
# |- interfaces.py           # NEW: Abstract base class for consumers
# |- kafka_consumer.py       # MOVED: Kafka-specific implementation
# |- file_consumer.py        # NEW: Example consumer for local testing
# |- writer.py               # Feast online store writing logic
# |- config.py               # Configuration management (updated)
# |- token_provider.py       # Custom OAuth token provider for Kafka
# |- schemas.py              # Avro and Pydantic schemas
# |
# |- feature_repo/
# |  |- ...
#
# |- test_data.jsonl         # NEW: Data file for FileConsumer
# |- requirements.txt        # Project dependencies

# --- 1. config.py ---
# Description: Manages application configuration. ADDED STREAMING_SOURCE setting.

import os
from pydantic_settings import BaseSettings
from typing import Literal


class AppConfig(BaseSettings):
    """General application settings."""
    # NEW: Determines which consumer implementation to use
    streaming_source: Literal["kafka", "file"] = os.environ.get("STREAMING_SOURCE", "kafka")

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class KafkaSettings(BaseSettings):
    """Configuration for Kafka connection."""
    bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "your-gcp-kafka-broker.com:9092")
    topic_name: str = os.environ.get("KAFKA_TOPIC_NAME", "user_activity_stream")
    group_id: str = os.environ.get("KAFKA_GROUP_ID", "feast_ingestion_group")
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "OAUTHBEARER"
    max_batch_size: int = int(os.environ.get("KAFKA_MAX_BATCH_SIZE", 1000))
    poll_timeout_secs: float = float(os.environ.get("KAFKA_POLL_TIMEOUT_SECS", 1.0))

    class Config:
        env_file = '.env'


class SchemaRegistrySettings(BaseSettings):
    """Configuration for Confluent Schema Registry."""
    url: str = os.environ.get("SCHEMA_REGISTRY_URL", "http://your-schema-registry:8081")

    class Config:
        env_file = '.env'


class FeastSettings(BaseSettings):
    """Configuration for Feast."""
    repo_path: str = os.path.join(os.getcwd(), "feature_repo")

    class Config:
        env_file = '.env'


class FileConsumerSettings(BaseSettings):
    """Configuration for the File consumer."""
    file_path: str = os.environ.get("FILE_CONSUMER_PATH", "test_data.jsonl")
    batch_size: int = int(os.environ.get("FILE_CONSUMER_BATCH_SIZE", 100))
    loop_delay_secs: int = int(os.environ.get("FILE_CONSUMER_LOOP_DELAY_SECS", 5))

    class Config:
        env_file = '.env'


class Settings:
    """Container for all application settings."""
    app: AppConfig = AppConfig()
    kafka: KafkaSettings = KafkaSettings()
    feast: FeastSettings = FeastSettings()
    schema_registry: SchemaRegistrySettings = SchemaRegistrySettings()
    file_consumer: FileConsumerSettings = FileConsumerSettings()


settings = Settings()

# --- 2. interfaces.py (NEW) ---
# Description: Defines the abstract interface for any streaming consumer.

from abc import ABC, abstractmethod


class StreamingConsumer(ABC):
    """
    An abstract base class that defines the interface for a streaming data consumer.
    Any class that consumes data from a source (like Kafka, Pub/Sub, etc.) should
    implement this interface.
    """

    @abstractmethod
    def consume(self):
        """
        Starts the consumer loop. This method should block and continuously poll
        for new data from the source, process it, and write it to the sink.
        """
        pass


# --- 3. token_provider.py ---
# (No changes here)
import time, logging


class GCPTokenProvider:
    def __init__(self): self._token, self._token_expiry_time = None, 0

    def get_token(self):
        if not self._token or time.time() >= self._token_expiry_time:
            self._token, self._token_expiry_time = "dummy-gcp-oauth-token-for-kafka", time.time() + 3600
        return self._token, self._token_expiry_time


token_provider_instance = GCPTokenProvider()


def oauth_token_provider_callback(cfg): return token_provider_instance.get_token()


# --- 4. schemas.py ---
# (No changes here)
from pydantic import BaseModel, Field
from datetime import datetime
import json

user_activity_avro_schema_str = json.dumps({
    "type": "record", "name": "UserActivity", "namespace": "com.yourcompany.stream",
    "fields": [
        {"name": "user_id", "type": "long"}, {"name": "activity_type", "type": "string"},
        {"name": "amount_spent", "type": "double"},
        {"name": "event_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
    ]
})


class UserActivity(BaseModel):
    user_id: int;
    activity_type: str;
    amount_spent: float;
    event_timestamp: datetime


# --- 5. feature_repo/ (No changes to contents) ---
# ... feature_store.yaml and definitions.py remain the same ...


# --- 6. writer.py ---
# (No changes here)
from feast import FeatureStore
import pandas as pd


class FeastWriterService:
    def __init__(self, repo_path: str):
        self.store = FeatureStore(repo_path=repo_path)

    def write(self, data: list[dict]):
        if not data: return
        try:
            self.store.write_to_online_store("user_activity_stream", pd.DataFrame(data))
            logging.info(f"Successfully wrote {len(data)} records to Feast.")
        except Exception as e:
            logging.error(f"Failed to write batch to Feast: {e}", exc_info=True); raise


# --- 7. kafka_consumer.py (REFACTORED from consumer.py) ---
# Description: The Kafka-specific implementation of the StreamingConsumer interface.

from confluent_kafka.avro import AvroConsumer
from confluent_kafka import Message
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from interfaces import StreamingConsumer
from schemas import UserActivity
from writer import FeastWriterService
from config import KafkaSettings, SchemaRegistrySettings


def process_single_message(msg: Message) -> Optional[Dict]:
    try:
        data = msg.value()
        data['event_timestamp'] = datetime.fromtimestamp(data['event_timestamp'] / 1000.0)
        return UserActivity(**data).model_dump()
    except Exception as e:
        logging.error(f"Error processing message: {e}"); return None


class KafkaConsumerService(StreamingConsumer):
    def __init__(self, kafka_config: KafkaSettings, schema_registry_config: SchemaRegistrySettings, token_callback,
                 writer: FeastWriterService):
        self.writer = writer;
        self.batch_size = kafka_config.max_batch_size;
        self.poll_timeout = kafka_config.poll_timeout_secs
        conf = {
            'bootstrap.servers': kafka_config.bootstrap_servers, 'group.id': kafka_config.group_id,
            'security.protocol': kafka_config.security_protocol, 'sasl.mechanism': kafka_config.sasl_mechanism,
            'sasl.oauthbearer.config': '', 'auto.offset.reset': 'earliest',
            'schema.registry.url': schema_registry_config.url, 'fetch.min.bytes': 1,
            'fetch.max.wait.ms': 500, 'max.partition.fetch.bytes': 2 * 1024 * 1024,
            'oauth_cb': token_callback
        }
        self.consumer = AvroConsumer(conf)
        self.topic = kafka_config.topic_name
        self.executor = ThreadPoolExecutor()
        logging.info(f"Kafka AvroConsumer configured for batch processing (size: {self.batch_size}).")

    def consume(self):
        self.consumer.subscribe([self.topic])
        logging.info(f"Subscribed to Kafka topic: {self.topic}")
        try:
            while True:
                messages = self.consumer.consume(num_messages=self.batch_size, timeout=self.poll_timeout)
                if not messages: continue
                valid_messages = [msg for msg in messages if msg.error() is None]
                if valid_messages: self._process_batch(valid_messages)
                self.consumer.commit(asynchronous=True)
        finally:
            self.consumer.close(); self.executor.shutdown(); logging.info("Kafka consumer shut down.")

    def _process_batch(self, messages: List[Message]):
        try:
            results = list(self.executor.map(process_single_message, messages))
            feature_rows = [res for res in results if res is not None]
            if feature_rows: self.writer.write(feature_rows)
        except Exception as e:
            logging.error(f"Failed to process a batch: {e}", exc_info=True)


# --- 8. file_consumer.py (NEW) ---
# Description: An example consumer that reads from a local file, useful for testing.

import json
import time
from typing import List, Dict, Optional
from interfaces import StreamingConsumer
from schemas import UserActivity
from writer import FeastWriterService
from config import FileConsumerSettings


class FileConsumerService(StreamingConsumer):
    def __init__(self, config: FileConsumerSettings, writer: FeastWriterService):
        self.config = config
        self.writer = writer
        logging.info(f"FileConsumer configured for path: {config.file_path}")

    def consume(self):
        logging.info("Starting file consumption loop...")
        try:
            while True:
                with open(self.config.file_path, 'r') as f:
                    batch = []
                    for line in f:
                        if len(batch) >= self.config.batch_size:
                            self._process_batch(batch)
                            batch = []
                        try:
                            batch.append(json.loads(line))
                        except json.JSONDecodeError:
                            logging.warning(f"Skipping malformed JSON line: {line}")
                    if batch:  # Process the final batch
                        self._process_batch(batch)
                logging.info(f"Finished reading file. Waiting {self.config.loop_delay_secs}s before next loop.")
                time.sleep(self.config.loop_delay_secs)
        except FileNotFoundError:
            logging.error(f"Fatal: Data file not found at {self.config.file_path}. Exiting.")
        except Exception as e:
            logging.critical(f"Critical error in file consumer loop: {e}", exc_info=True)

    def _process_batch(self, batch: List[Dict]):
        feature_rows = []
        for record in batch:
            try:
                # Timestamps in JSON are strings, need parsing
                record['event_timestamp'] = datetime.fromisoformat(record['event_timestamp'])
                activity = UserActivity(**record)
                feature_rows.append(activity.model_dump())
            except Exception as e:
                logging.error(f"Error processing record {record}: {e}")

        if feature_rows:
            self.writer.write(feature_rows)


# --- 9. main.py (REFACTORED) ---
# Description: Main FastAPI app with a factory to select the consumer.

import asyncio
import logging
from fastapi import FastAPI, status
from config import settings
from interfaces import StreamingConsumer
from writer import FeastWriterService

# Import concrete implementations
from kafka_consumer import KafkaConsumerService
from file_consumer import FileConsumerService
from token_provider import oauth_token_provider_callback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = FastAPI(
    title="Generic Streaming to Feast Service",
    description="Consumes from a configured stream (Kafka/File) and writes to Feast.",
    version="2.0.0"
)


def get_consumer() -> StreamingConsumer:
    """Factory function to get the correct consumer based on config."""
    writer = FeastWriterService(repo_path=settings.feast.repo_path)
    source = settings.app.streaming_source

    if source == "kafka":
        logging.info("Initializing KafkaConsumerService.")
        return KafkaConsumerService(
            kafka_config=settings.kafka,
            schema_registry_config=settings.schema_registry,
            token_callback=oauth_token_provider_callback,
            writer=writer
        )
    elif source == "file":
        logging.info("Initializing FileConsumerService.")
        return FileConsumerService(config=settings.file_consumer, writer=writer)
    else:
        raise ValueError(f"Unsupported streaming_source: '{source}'")


# Initialize the consumer using the factory
consumer_service = get_consumer()


@app.on_event("startup")
async def startup_event():
    logging.info("Application startup...")
    asyncio.create_task(run_consumer())


async def run_consumer():
    loop = asyncio.get_event_loop()
    # The consumer's consume() method is blocking, so run it in an executor
    await loop.run_in_executor(None, consumer_service.consume)


@app.get("/health", status_code=status.HTTP_200_OK, tags=["Monitoring"])
async def health_check():
    return {"status": "ok", "source": settings.app.streaming_source}

# --- 10. test_data.jsonl (NEW) ---
# Example data for the FileConsumer. One JSON object per line.
# {"user_id": 1001, "activity_type": "login", "amount_spent": 0.0, "event_timestamp": "2025-06-12T15:52:01Z"}
# {"user_id": 1002, "activity_type": "purchase", "amount_spent": 19.99, "event_timestamp": "2025-06-12T15:52:05Z"}
# {"user_id": 1001, "activity_type": "search", "amount_spent": 0.0, "event_timestamp": "2025-06-12T15:52:10Z"}


# --- 11. requirements.txt ---
# (No changes)
# fastapi
# uvicorn
# pydantic
# pydantic-settings
# confluent-kafka[avro]
# feast
# pandas
