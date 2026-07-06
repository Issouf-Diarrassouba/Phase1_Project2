import random
import uuid
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
    for _ in range(5):
        print(generate_event())