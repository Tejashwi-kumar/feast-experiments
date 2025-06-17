# kafka_feature_consumer_oauth.py

import argparse
import io
import json
import pandas as pd

# Confluent Kafka library
from confluent_kafka import Consumer, KafkaException, KafkaError

# --- Imports for GCP OAuth and fastavro Serialization ---
import google.auth
import google.auth.transport.requests
from fastavro import schemaless_reader


# --- Class for GCP OAuth Token Handling (Reused from producer) ---
# This class must be identical to the one used by the producer.
class GcpOauthCallback:
    """
    This class is a callable that provides OAuth tokens for the Kafka client.
    It automatically handles fetching and refreshing the token from Google's
    metadata server or a local service account file.
    """

    def __init__(self):
        print("Initializing GCP credentials...")
        self._creds, self._project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        self._request = google.auth.transport.requests.Request()
        self._token = None
        self._token_expiry = 0

    def __call__(self, config_str):
        """
        This method is invoked by the Kafka client when it needs a token.
        """
        import time
        if time.time() > self._token_expiry - 60:
            print("OAuth token is expiring or expired, refreshing...")
            self._creds.refresh(self._request)
            self._token = self._creds.token
            if self._creds.expiry:
                self._token_expiry = self._creds.expiry.timestamp()
                print(f"Successfully refreshed OAuth token. New expiry: {self._creds.expiry}")
            else:
                self._token_expiry = time.time() + 3300
                print("Successfully refreshed OAuth token. No expiry in credential, using default.")
        return self._token, self._token_expiry * 1000


# --- Avro Schema (Must match the producer's schema exactly) ---
AVRO_SCHEMA_STR = """
{
    "type": "record",
    "name": "featuredata",
    "namespace": "com.yourcompany.stream",
    "fields": [
        {
            "name": "attributes",
            "type": {
                "type": "array",
                "items": "string"
            }
        },
        {
            "name": "feature_id",
            "type": "int"
        },
        {
            "name": "ts",
            "type": "string"
        },
        {
            "name": "count",
            "type": "long"
        }
    ]
}
"""


def deserialize_avro(schema, avro_bytes):
    """
    Deserializes Avro binary format into a Python dictionary using fastavro.
    """
    bytes_reader = io.BytesIO(avro_bytes)
    # Use schemaless_reader since the schema is known by the consumer
    # and not embedded in the message.
    record = schemaless_reader(bytes_reader, schema)
    return record


def main(args):
    """Main function to set up the consumer and process messages."""
    topic = args.topic

    # Parse the Avro schema once at the beginning using a JSON load.
    parsed_schema = json.loads(AVRO_SCHEMA_STR)

    # --- Consumer Configuration with OAUTHBEARER ---
    consumer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning of the topic
        'enable.auto.commit': False,  # We will commit offsets manually

        # --- SASL/OAUTHBEARER Configuration ---
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': GcpOauthCallback()
    }

    print("Initializing Kafka consumer with OAUTHBEARER...")
    consumer = Consumer(consumer_config)
    print("Consumer initialized.")

    # List to hold batches of records before loading into DataFrame
    records_list = []
    batch_size = 100  # Process messages in batches of 100

    try:
        consumer.subscribe([topic])
        print(f"Subscribed to topic '{topic}'. Waiting for messages... Press Ctrl+C to exit.")

        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message received
                key = msg.key().decode('utf-8')
                value = msg.value()

                # Deserialize the Avro message
                deserialized_record = deserialize_avro(parsed_schema, value)
                print(f"Consumed record with key '{key}': {deserialized_record}")

                records_list.append(deserialized_record)

                # Once the batch is full, process it
                if len(records_list) >= batch_size:
                    print(f"\n--- Creating DataFrame from batch of {len(records_list)} records ---")
                    df = pd.DataFrame(records_list)
                    print(df.head())
                    print("--------------------------------------------------\n")

                    # Clear the list for the next batch
                    records_list.clear()

                # Manually commit the offset after processing.
                consumer.commit(asynchronous=True)

    except KeyboardInterrupt:
        print("Aborted by user.")
    finally:
        # Close down consumer to commit final offsets.
        print("\nClosing consumer...")
        consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Avro Consumer with GCP OAuth")
    parser.add_argument('-b', '--bootstrap-servers', required=True, help='Kafka bootstrap server(s)')
    parser.add_argument('-t', '--topic', default='feature_events', help='Kafka topic to consume from')
    parser.add_argument('-g', '--group', default='feature_consumer_group_1', help='Consumer group ID')
    args = parser.parse_args()

    main(args)
