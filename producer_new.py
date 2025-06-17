# kafka_feature_producer.py

import uuid
import random
from datetime import datetime, timezone
import argparse
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Define the Avro schema for the Kafka message value.
# This schema matches the structure specified in the requirements.
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
# In a real-world scenario, this might come from a config file or a database.
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


def delivery_report(err, msg):
    """
    Callback function to report the result of a produce operation.
    This is called once for each message produced.
    """
    if err is not None:
        print(f"Message delivery failed for key {msg.key().decode('utf-8')}: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition: {msg.partition()}] @ Offset {msg.offset()}")
        print(f" -> Key: {msg.key().decode('utf-8')}")


def main(args):
    """
    Main function to set up the producer and send messages.
    """
    topic = args.topic

    # --- 1. Configure Serializer ---
    # Set up the Schema Registry client and the Avro serializer for the message value.
    schema_registry_client = SchemaRegistryClient({'url': args.schema_registry})
    avro_serializer = AvroSerializer(
        schema_registry_client,
        AVRO_SCHEMA_STR
    )

    # --- 2. Configure Producer ---
    # The key will be a string, and the value will be Avro-serialized.
    producer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        # 'linger.ms': 10, # Optional: wait 10ms to batch messages
        # 'compression.type': 'snappy', # Optional: use compression
    }
    producer = SerializingProducer(producer_config)

    print(f"Producing records for {len(FEATURES)} features to topic '{topic}'. Press Ctrl+C to exit.")

    try:
        # --- 3. Generate and Produce Messages ---
        # Loop through every defined feature to create a message for it.
        for feature_name, feature_id in FEATURE_ID_MAP.items():

            # A. Generate a message for the 'device_id' entity
            device_id = f"device-{uuid.uuid4()}"
            device_message = {
                "attributes": [device_id],
                "feature_id": feature_id,
                "ts": datetime.now(timezone.utc).isoformat(),
                "count": random.randint(1, 1000)
            }
            # The partition key is the unique combination of the entity and the feature.
            device_key = f"{device_id}-{feature_id}"
            producer.produce(
                topic=topic,
                key=device_key,
                value=device_message,
                on_delivery=delivery_report
            )

            # B. Generate a message for the standalone 'subscriber_id' entity
            subscriber_id = f"sub-{random.randint(100000, 999999)}"
            subscriber_message = {
                "attributes": [subscriber_id],
                "feature_id": feature_id,
                "ts": datetime.now(timezone.utc).isoformat(),
                "count": random.randint(1, 1000)
            }
            subscriber_key = f"{subscriber_id}-{feature_id}"
            producer.produce(
                topic=topic,
                key=subscriber_key,
                value=subscriber_message,
                on_delivery=delivery_report
            )

            # C. Generate a message for the combined 'subscriber_id:account_id' entity
            sub_id_for_combo = f"sub-{random.randint(100000, 999999)}"
            acc_id_for_combo = f"acc-{random.randint(5000, 9999)}"
            combined_entity = f"{sub_id_for_combo}:{acc_id_for_combo}"
            combined_message = {
                "attributes": [combined_entity],
                "feature_id": feature_id,
                "ts": datetime.now(timezone.utc).isoformat(),
                "count": random.randint(1, 1000)
            }
            combined_key = f"{combined_entity}-{feature_id}"
            producer.produce(
                topic=topic,
                key=combined_key,
                value=combined_message,
                on_delivery=delivery_report
            )

            # Poll for events. This is non-blocking and handles delivery reports.
            producer.poll(0)

    except KeyboardInterrupt:
        print("Aborted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # --- 4. Flush and Cleanup ---
        # Wait for any outstanding messages to be delivered and delivery reports to be received.
        print("\nFlushing producer...")
        producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Avro Producer for Feature Events")
    parser.add_argument('-b', '--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap server(s)')
    parser.add_argument('-s', '--schema-registry', default='http://localhost:8081', help='Schema Registry URL')
    parser.add_argument('-t', '--topic', default='feature_events', help='Kafka topic to produce to')
    args = parser.parse_args()

    main(args)