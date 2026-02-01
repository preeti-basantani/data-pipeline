import logging
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Dict, Any

def create_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    """
    Create a Kafka producer instance.
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
    Returns:
        KafkaProducer: Configured Kafka producer
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=0  # We handle retries manually
    )

def send_event_with_retry(
    producer: KafkaProducer,
    topic: str,
    event: Dict[str, Any],
    max_retries: int = 5,
    backoff_seconds: int = 2,
) -> bool:
    """
    Send an event message to a Kafka topic with retry and error handling.
    Args:
        producer (KafkaProducer): Producer instance
        topic (str): Kafka topic name
        event (dict): The event payload
        max_retries (int): Max number of publish retries
        backoff_seconds (int): Seconds to wait before retrying
    Returns:
        bool: True if successful, False if failed after retries
    """
    attempt = 0
    while attempt <= max_retries:
        try:
            future = producer.send(topic, value=event)
            future.get(timeout=10)
            logging.info(f"Message published to topic '{topic}' in attempt {attempt + 1}.")
            return True
        except KafkaError as e:
            logging.error(f"Failed to publish message to '{topic}' (attempt {attempt + 1}): {e}")
            attempt += 1
            time.sleep(backoff_seconds)
    logging.error(f"Exceeded max retries ({max_retries}) for topic '{topic}'. Giving up.")
    return False

# Example usage (remove or adapt for production pipelines)
if __name__ == "__main__":
    producer = create_producer()
    event = {"transaction_id": 123, "amount": 100.0}
    send_event_with_retry(producer, "transactions", event)
    producer.close()
