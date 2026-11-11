"""Unit tests for MQTT ingestion, topic parsing, message validation, and InfluxDB point building."""

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from ingestor import (
    TelemetryIngestor,
    parse_timestamp,
    parse_topic,
    validate_payload,
)


class TestTopicParsing:
    """Test suite for MQTT topic extraction."""

    def test_valid_topics(self):
        result = parse_topic("factory/machine_1/temperature", prefix="factory")
        assert result == ("machine_1", "temperature")

        result2 = parse_topic("factory/press_line_2/vibration", prefix="factory")
        assert result2 == ("press_line_2", "vibration")

    def test_custom_prefix(self):
        result = parse_topic("plant_north/cnc_01/pressure", prefix="plant_north")
        assert result == ("cnc_01", "pressure")

    def test_invalid_topic_structure(self):
        # Missing sensor part
        assert parse_topic("factory/machine_1", prefix="factory") is None
        # Too many parts
        assert parse_topic("factory/machine_1/sensor/sub", prefix="factory") is None
        # Wrong prefix
        assert parse_topic("other_prefix/machine_1/temp", prefix="factory") is None
        # Empty or slash only
        assert parse_topic("", prefix="factory") is None
        assert parse_topic("///", prefix="factory") is None


class TestPayloadValidation:
    """Test suite for telemetry schema validation."""

    def test_valid_payload(self):
        payload = {
            "machine_id": "machine_1",
            "sensor_id": "temperature",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": 65.4,
            "unit": "celsius",
        }
        assert validate_payload(payload) is True

    def test_valid_payload_with_integer_value(self):
        payload = {
            "machine_id": "machine_1",
            "sensor_id": "pressure",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": 5,
        }
        assert validate_payload(payload) is True

    def test_missing_required_fields(self):
        base = {
            "machine_id": "machine_1",
            "sensor_id": "temperature",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": 65.4,
        }
        for field in ["machine_id", "sensor_id", "timestamp", "value"]:
            incomplete = dict(base)
            del incomplete[field]
            assert validate_payload(incomplete) is False

    def test_reject_boolean_value(self):
        # Python bool is an int subclass (isinstance(True, int) == True), must be explicitly rejected
        payload = {
            "machine_id": "m1",
            "sensor_id": "temp",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": True,
        }
        assert validate_payload(payload) is False

    def test_reject_string_value(self):
        payload = {
            "machine_id": "m1",
            "sensor_id": "temp",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": "65.4",
        }
        assert validate_payload(payload) is False

    def test_reject_non_dict(self):
        assert validate_payload("not a json object") is False
        assert validate_payload([1, 2, 3]) is False
        assert validate_payload(None) is False


class TestTimestampParsing:
    """Test suite for timestamp normalization."""

    def test_iso_utc_string(self):
        dt = parse_timestamp("2026-11-10T14:30:00Z")
        assert dt.year == 2026
        assert dt.hour == 14
        assert dt.tzinfo is not None

    def test_unix_timestamp(self):
        dt = parse_timestamp(1762785000.0)
        assert isinstance(dt, datetime)
        assert dt.tzinfo is not None

    def test_fallback_on_corrupt_timestamp(self):
        dt = parse_timestamp("invalid-date-format")
        assert isinstance(dt, datetime)


class TestTelemetryIngestorFlow:
    """Test full message ingestion and InfluxDB point building."""

    @pytest.fixture
    def ingestor(self):
        ing = TelemetryIngestor()
        # Mock InfluxDB write API so no actual network call is made
        ing.write_api = MagicMock()
        return ing

    def test_process_message_success(self, ingestor):
        topic = "factory/machine_1/vibration"
        raw_msg = json.dumps({
            "machine_id": "machine_1",
            "sensor_id": "vibration",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": 2.65,
            "unit": "mm/s",
        }).encode("utf-8")

        point = ingestor.process_message(topic, raw_msg)
        assert point is not None
        # Convert point to line protocol to inspect tags and fields
        line = point.to_line_protocol()

        assert "sensor_telemetry" in line
        assert "machine_id=machine_1" in line
        assert "sensor_id=vibration" in line
        assert "value=2.65" in line
        assert "is_anomaly=0" in line
        assert ingestor.messages_processed == 1

    def test_process_message_ignores_malformed_json(self, ingestor):
        topic = "factory/machine_1/temperature"
        raw_msg = b"NOT_VALID_JSON{{"

        point = ingestor.process_message(topic, raw_msg)
        assert point is None
        assert ingestor.messages_processed == 0

    def test_process_message_ignores_invalid_topic(self, ingestor):
        topic = "invalid/topic"
        raw_msg = json.dumps({"value": 10.0}).encode("utf-8")

        point = ingestor.process_message(topic, raw_msg)
        assert point is None
        assert ingestor.messages_processed == 0

    def test_process_message_flags_anomaly(self, ingestor):
        # Pre-seed detector for this stream with normal values
        det = ingestor.get_detector("machine_1", "temperature")
        for _ in range(15):
            det.detect(65.0)

        # Inject extreme spike
        topic = "factory/machine_1/temperature"
        raw_msg = json.dumps({
            "machine_id": "machine_1",
            "sensor_id": "temperature",
            "timestamp": "2026-11-10T14:00:00Z",
            "value": 150.0,
            "unit": "celsius",
        }).encode("utf-8")

        point = ingestor.process_message(topic, raw_msg)
        assert point is not None
        line = point.to_line_protocol()

        assert "is_anomaly=1" in line
        assert ingestor.anomalies_detected == 1
