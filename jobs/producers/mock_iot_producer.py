import os
import random
import uuid
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timezone

DEVICES = [
    ("dev_101", "living_room"),
    ("dev_102", "bedroom"),
    ("dev_201", "kitchen"),
    ("dev_301", "hallway"),
]

EVENT_RULES = {
    "temperature": (-20, 130, "F"),
    "humidity": (0, 100, "%"),
    "motion": (0, 1, "Boolean"),
    "smoke": (0, 1, "Boolean"),
    "battery": (0, 100, "%"),
}

def generate_event():
    event_type = random.choice(list(EVENT_RULES.keys()))
    low, high, unit = EVENT_RULES[event_type]
    device_id, room = random.choice(DEVICES)

    if event_type in ("motion", "smoke"):
        value = float(random.randint(0, 1))
    else:
        value = round(random.uniform(low, high), 1)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "device_id": device_id,
        "room": room,
        "source": "simulator",
        "value": value,
        "unit": unit,
    }

if __name__ == "__main__":
    # Use the broker address from the environment (compose sets this to
    # redpanda:9092). Falls back to localhost for running outside Docker.
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"[producer] connected to {bootstrap_servers}, publishing to smart-home.events")

    while True:
        event = generate_event()
        producer.send("smart-home.events", value=event)
        print(f"Sent: {event}")
        time.sleep(1)
