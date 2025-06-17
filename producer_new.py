# kafka_feature_producer_oauth.py

import uuid
import random
import time
from datetime import datetime, timezone
import argparse
import io

# Confluent Kafka library
from confluent_kafka import Producer

# --- New Imports for GCP OAuth and Manual Avro Serialization ---
import google.auth
import google.auth.transport.requests
import avro.schema
import avro.io


# --- Class for GCP OAuth Token Handling (Unchanged) ---
class GcpOauthCallback:
    """
    This class is a callable that provides OAuth tokens for the Kafka client.
    It automatically handles fetching and refreshing the token from Google's
    metadata server or a local service account file.
    """
    def __init__(self):
        # Use google.auth.default() to find credentials automatically in the environment
        # (e.g., GCE metadata server, GKE Workload Identity, GOOGLE_APPLICATION_CREDENTIALS)
        print("Initializing GCP credentials...")
        self._creds, self._project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        self._request = google.auth.transport.requests.Request()
        self._token = None
        # The token expiry time, as a Unix timestamp.
        self._token_expiry = 0

    def __call__(self, config_str):
        """
        This method is invoked by the Kafka client when it needs a token.
        It returns the token and its expiration time in milliseconds.
        """
        # Refresh the token if it's expired or about to expire (60s buffer).
        if time.time() > self._token_expiry - 60:
            print("OAuth token is expiring or expired, refreshing...")
            self._creds.refresh(self._request)
            self._token = self._creds.token
            if self._creds.expiry:
                 self._token_expiry = self._creds.expiry.timestamp()
                 print(f"Successfully refreshed OAuth token. New expiry: {self._creds.expiry}")
            else:
                # Set a default expiry if not provided, e.g., 55 minutes from now.
                self._token_expiry = time.time() + 3300
                print("Successfully refreshed OAuth token. No expiry in credential, using default.")

        # The Kafka client expects a tuple of (token_string, expiry_time_ms)
        return self._token, self._token_expiry * 1000


# Define the Avro schema for the Kafka message value.
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

# Define the list of 25 features.
FEATURES = [
    "count_ao_events_1min", "count_ao_events_5min", "count_ao_events_15min",
    "device_real_loc", "user_avg_spend_1d", "user_avg_spend_7d",
    "login_attempts_1h", "failed_logins_1h", "password_resets_24h",
    "session_duration_avg_1d", "session_duration_max_1d", "items_in_cart",
    "cart_value", "viewed_product_categories_1h", "clicks_per_minute",
    "time_since_last_event", "is_guest_user", "has_promo_code",
    "payment_method_type", "shipping_country", "app_version",
    "device_os_version", "network_type", "is_vpn_active", "feature_25"
]

# Create a mapping from feature name to a unique integer ID, starting from 1001.
FEATURE_ID_MAP = {name: i for i, name in enumerate(FEATURES, 1001)}


def serialize_avro(schema, message_dict):
    """
    Serializes a Python dictionary into Avro binary format based on a given schema.
    """
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(message_dict, encoder)
    return bytes_writer.getvalue()


def delivery_report(err, msg):
    """Callback function to report the result of a produce operation."""
    if err is not None:
        print(f"Message delivery failed for key {msg.key().decode('utf-8')}: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition: {msg.partition()}] @ Offset {msg.offset()}")
        print(f" -> Key: {msg.key().decode('utf-8')}")


def main(args):
    """Main function to set up the producer and send messages."""
    topic = args.topic

    # Parse the Avro schema once at the beginning.
    avro_schema = avro.schema.parse(AVRO_SCHEMA_STR)

    # --- Updated Producer Configuration (No Serializers) ---
    producer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        # --- SASL/OAUTHBEARER Configuration ---
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': GcpOauthCallback()
    }

    print("Initializing Kafka producer with OAUTHBEARER...")
    # Use the base Producer, as we are providing already-serialized bytes.
    producer = Producer(producer_config)
    print("Producer initialized.")

    print(f"Producing records for {len(FEATURES)} features to topic '{topic}'. Press Ctrl+C to exit.")

    try:
        # Generate and Produce Messages
        for feature_name, feature_id in FEATURE_ID_MAP.items():
            # A. 'device_id' entity
            device_id = f"device-{uuid.uuid4()}"
            device_message = {"attributes": [device_id], "feature_id": feature_id, "ts": datetime.now(timezone.utc).isoformat(), "count": random.randint(1, 1000)}
            device_key = f"{device_id}-{feature_id}"
            serialized_value = serialize_avro(avro_schema, device_message)
            producer.produce(topic=topic, key=device_key.encode('utf-8'), value=serialized_value, on_delivery=delivery_report)

            # B. 'subscriber_id' entity
            subscriber_id = f"sub-{random.randint(100000, 999999)}"
            subscriber_message = {"attributes": [subscriber_id], "feature_id": feature_id, "ts": datetime.now(timezone.utc).isoformat(), "count": random.randint(1, 1000)}
            subscriber_key = f"{subscriber_id}-{feature_id}"
            serialized_value = serialize_avro(avro_schema, subscriber_message)
            producer.produce(topic=topic, key=subscriber_key.encode('utf-8'), value=serialized_value, on_delivery=delivery_report)

            # C. 'subscriber_id:account_id' entity
            sub_id_for_combo, acc_id_for_combo = f"sub-{random.randint(100000, 999999)}", f"acc-{random.randint(5000, 9999)}"
            combined_entity = f"{sub_id_for_combo}:{acc_id_for_combo}"
            combined_message = {"attributes": [combined_entity], "feature_id": feature_id, "ts": datetime.now(timezone.utc).isoformat(), "count": random.randint(1, 1000)}
            combined_key = f"{combined_entity}-{feature_id}"
            serialized_value = serialize_avro(avro_schema, combined_message)
            producer.produce(topic=topic, key=combined_key.encode('utf-8'), value=serialized_value, on_delivery=delivery_report)

            producer.poll(0)

    except KeyboardInterrupt:
        print("Aborted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nFlushing producer...")
        producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Avro Producer for Feature Events with GCP OAuth")
    parser.add_argument('-b', '--bootstrap-servers', required=True, help='Kafka bootstrap server(s)')
    # The --schema-registry argument has been removed.
    parser.add_argument('-t', '--topic', default='feature_events', help='Kafka topic to produce to')
    args = parser.parse_args()

    main(args)

