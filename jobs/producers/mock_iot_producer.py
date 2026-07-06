"""
mock_iot_producer.py

Continuous (or bounded) synthetic sensor stream generator. Publishes JSON
IoT telemetry events to the Redpanda topic "iot-telemetry" (Kafka protocol
compatible, so kafka-python works against it without modification).

Usage:
    # Run forever, ~5 messages/sec, until Ctrl+C
    python mock_iot_producer.py

    # Run a fixed number of messages then exit (used by the Airflow DAG
    # iot_bounded_producer.py for reproducible batch runs)
    python mock_iot_producer.py --count 500 --rate 20

Environment variables (see .env):
    BROKER_ADDRESS   Kafka/Redpanda bootstrap address, e.g. "localhost:19092"
    TOPIC_NAME       Target topic, defaults to "iot-telemetry"
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DEVICE_TYPES = ["temp-humidity", "pressure", "multi-sensor"]
LOCATIONS = ["warehouse-a", "warehouse-b", "loading-dock", "cold-storage", "rooftop"]
FIRMWARE_VERSIONS = ["1.4.2", "1.4.3", "1.5.0", "2.0.0-beta"]

_shutdown = False


def _handle_sigint(signum, frame):
    global _shutdown
    _shutdown = True


def build_producer(broker_address):
    return KafkaProducer(
        bootstrap_servers=broker_address,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=20,
    )


def generate_device_fleet(fleet_size=25):
    """Pre-assign stable device_id -> (device_type, location) mappings so
    readings from the 'same' sensor look consistent across the run."""
    fleet = []
    for i in range(fleet_size):
        fleet.append({
            "device_id": f"sensor-{i:04d}-{uuid.uuid4().hex[:6]}",
            "device_type": random.choice(DEVICE_TYPES),
            "location": random.choice(LOCATIONS),
            "firmware_version": random.choice(FIRMWARE_VERSIONS),
        })
    return fleet


def generate_reading(device, inject_bad_data_rate=0.02):
    """Build one telemetry event for a given device.

    inject_bad_data_rate: probability of emitting a deliberately malformed
    record (missing required field), to exercise the consumer's DLQ path.
    """
    now = datetime.now(timezone.utc)

    event = {
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "location": device["location"],
        "event_time": now.isoformat(),
        "temperature_c": round(random.uniform(-5.0, 45.0), 2),
        "humidity_pct": round(random.uniform(10.0, 95.0), 2),
        "pressure_hpa": round(random.uniform(980.0, 1040.0), 2),
        "battery_pct": round(random.uniform(0.0, 100.0), 2),
        "signal_strength_dbm": random.randint(-100, -40),
        "firmware_version": device["firmware_version"],
    }

    if random.random() < inject_bad_data_rate:
        # Simulate real-world corrupt payloads: drop a required field
        corrupt_field = random.choice(["device_id", "device_type", "event_time"])
        event.pop(corrupt_field, None)

    return event


def run(broker_address, topic, count, rate_per_sec, fleet_size):
    producer = build_producer(broker_address)
    fleet = generate_device_fleet(fleet_size)
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0

    sent = 0
    signal.signal(signal.SIGINT, _handle_sigint)

    print(f"[mock_iot_producer] streaming to topic='{topic}' via {broker_address} "
          f"(count={'infinite' if count is None else count}, rate={rate_per_sec}/s)")

    try:
        while not _shutdown and (count is None or sent < count):
            device = random.choice(fleet)
            reading = generate_reading(device)
            try:
                producer.send(topic, key=device["device_id"], value=reading)
                sent += 1
                if sent % 50 == 0:
                    print(f"[mock_iot_producer] sent {sent} messages")
            except KafkaError as e:
                print(f"[mock_iot_producer] send failed: {e}", file=sys.stderr)

            if delay:
                time.sleep(delay)
    finally:
        producer.flush(timeout=10)
        producer.close()
        print(f"[mock_iot_producer] shutting down. total sent={sent}")

    return sent


def main():
    parser = argparse.ArgumentParser(description="Synthetic IoT telemetry producer for Redpanda.")
    parser.add_argument("--broker", default=os.environ.get("BROKER_ADDRESS", "localhost:19092"),
                         help="Kafka/Redpanda bootstrap server address")
    parser.add_argument("--topic", default=os.environ.get("TOPIC_NAME", "iot-telemetry"),
                         help="Target topic name")
    parser.add_argument("--count", type=int, default=None,
                         help="Number of messages to send. Omit for continuous streaming.")
    parser.add_argument("--rate", type=float, default=5.0,
                         help="Target messages per second")
    parser.add_argument("--fleet-size", type=int, default=25,
                         help="Number of simulated devices")
    args = parser.parse_args()

    run(args.broker, args.topic, args.count, args.rate, args.fleet_size)


if __name__ == "__main__":
    main()