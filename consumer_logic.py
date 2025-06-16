# app/consumer.py
import multiprocessing
import json
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from feast import FeatureStore, StreamFeatureView


class KafkaConsumerProcess(multiprocessing.Process):
    """
    A dedicated process for a single consumer group that subscribes to a Kafka topic,
    filters messages for a specific feature_id, and ingests them into Feast.
    """

    def __init__(self, feature_store_path: str, feature_view_name: str, feature_id_to_consume: str):
        super().__init__()
        self.feature_store_path = feature_store_path
        self.feature_view_name = feature_view_name
        self.feature_id_to_consume = feature_id_to_consume
        self.daemon = True  # Allows main process to exit even if this one is running

    def run(self):
        """
        The main loop for the consumer process.
        """
        print(f"[Process {self.pid}] Starting consumer for Feature ID: {self.feature_id_to_consume}")

        # Each process must initialize its own FeatureStore instance.
        store = FeatureStore(repo_path=self.feature_store_path)
        fv = store.get_stream_feature_view(self.feature_view_name)
        kafka_source = fv.source

        consumer_config = {
            'bootstrap.servers': kafka_source.kafka_bootstrap_servers,
            'group.id': f"feast_ingestion_group_{self.feature_id_to_consume}",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # Manual offset commit for better control
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([kafka_source.topic])

        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        print(f"[Process {self.pid}] Kafka Error: {msg.error()}")
                        # Sleep on error to prevent tight loop failures
                        time.sleep(5)
                        break

                try:
                    # 1. Decode and filter the message
                    message_data = json.loads(msg.value().decode('utf-8'))

                    if message_data.get("feature_id") == self.feature_id_to_consume:
                        # 2. Prepare data for Feast ingestion
                        # The message itself is the feature row
                        feature_row = {
                            "event_timestamp": message_data["event_timestamp"],
                            "created_timestamp": message_data["created_timestamp"],
                            fv.entities[0].name: message_data[fv.entities[0].name],  # Assumes one entity per FV
                            fv.features[0].name: message_data[fv.features[0].name]  # Assumes one feature per FV
                        }

                        # 3. Write to the online store
                        store.write_to_online_store(
                            feature_view_name=self.feature_view_name,
                            feature_values=[feature_row]
                        )

                        print(f"[Process {self.pid}] Ingested: {feature_row}")

                        # 4. Manually commit offset after successful processing
                        consumer.commit(asynchronous=True)

                except json.JSONDecodeError:
                    print(f"[Process {self.pid}] Error decoding message: {msg.value()}")
                except Exception as e:
                    print(f"[Process {self.pid}] Error processing message: {e}")

        except KeyboardInterrupt:
            print(f"[Process {self.pid}] Shutting down consumer.")
        finally:
            consumer.close()
