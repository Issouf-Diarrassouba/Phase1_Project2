from jobs.producers.mock_iot_producer import generate_event

def test_event_has_required_fields():
    event = generate_event()
    required_fields = ["event_id", "event_type", "event_ts", "device_id", "room", "source", "value", "unit"]
    for field in required_fields:
        assert field in event

def test_event_type_is_valid():
    event = generate_event()
    assert event["event_type"] in ["temperature", "humidity", "motion", "smoke", "battery"]

def test_value_is_numeric():
    event = generate_event()
    assert isinstance(event["value"], float)