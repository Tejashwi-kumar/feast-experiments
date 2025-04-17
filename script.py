import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
import datetime


def generate_random_message():
    """Generates a random JSON message with a timestamp, driver_day_average, and driver_month_average."""

    # Generate random float values for averages
    driver_id = random.choice([1005,1006,1007])
    conv_rate = random.uniform(8.20, 15.70)
    acc_rate = random.uniform(20.15, 30.27)

    # Get current timestamp in ISO format
    timestamp = datetime.datetime.now().isoformat()

    # Construct the message as a dictionary
    message = {
        "driver_id":driver_id,
        "event_timestamp": timestamp,
        "conv_rate": conv_rate,
        "acc_rate": acc_rate,
    }

    # Convert the dictionary to a JSON string
    json_message = json.dumps(message)
    return json_message


def publish_message(producer, topic_name, message):
    """
    Publishes a message to a Kafka topic.

    Args:
        producer (KafkaProducer):  The Kafka producer instance.
        topic_name (str): The name of the Kafka topic.
        message (str): The message to publish.
    """
    try:
        # Encode the message as bytes
        message_bytes = message.encode('utf-8')

        # Send the message asynchronously
        future = producer.send(topic_name, message_bytes)

        # Block until the message is sent (optional, for synchronous sending)
        record_metadata = future.get(timeout=10)  # Adjust timeout as needed

        print(
            f"Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")

    except KafkaError as e:
        print(f"Error publishing message: {e}")


def create_kafka_producer(bootstrap_servers):
    """
    Creates a Kafka producer instance.

    Args:
        bootstrap_servers (str): Comma-separated list of Kafka broker addresses.

    Returns:
        KafkaProducer: A Kafka producer instance.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            # Optional: Add other producer configurations as needed
            # acks=1,  # 0, 1, or all
            # retries=3,
            # linger_ms=20,
        )
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None


if __name__ == '__main__':
    # Kafka broker address
    bootstrap_servers = "localhost:9092"  # Replace with your Kafka broker addresses

    # Kafka topic to publish to
    topic_name = "feast-ingestion"  # Replace with your desired topic name

    # Create a Kafka producer
    producer = create_kafka_producer(bootstrap_servers)

    if producer:
        # Continuously generate and publish messages
        try:
            while True:
                message = generate_random_message()
                publish_message(producer, topic_name, message)
                time.sleep(5)  # Send a message every 2 seconds (adjust as needed)
        except KeyboardInterrupt:
            print("Stopping producer...")
        finally:
            # Ensure the producer is closed when done
            producer.close()
            print("Kafka producer closed.")
    else:
        print("Failed to create Kafka producer. Exiting.")
