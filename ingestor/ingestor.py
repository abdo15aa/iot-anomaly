"""IoT Telemetry Ingestor & Real-Time Anomaly Detector.

Subscribes to MQTT telemetry topics (factory/<machine>/<sensor>),
parses and validates JSON payloads, executes streaming anomaly detection
(Z-Score or Isolation Forest), and writes enriched time-series points to InfluxDB 2.x.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as mqtt

from detectors import BaseDetector, create_detector

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Ingestor] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingestor")

# ------------------------------------------------------------------------------
# Configuration loaded from environment variables
# ------------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "factory")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "iot-anomaly-secret-auth-token-987654321")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "iot_org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_telemetry")

DETECTOR_TYPE = os.getenv("INGESTOR_DETECTOR_TYPE", "zscore")
ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "3.0"))
ZSCORE_WINDOW_SIZE = int(os.getenv("ZSCORE_WINDOW_SIZE", "30"))
IFOREST_WARMUP_SAMPLES = int(os.getenv("IFOREST_WARMUP_SAMPLES", "40"))
IFOREST_CONTAMINATION = float(os.getenv("IFOREST_CONTAMINATION", "0.05"))


def parse_topic(topic: str, prefix: str = MQTT_TOPIC_PREFIX) -> Optional[Tuple[str, str]]:
    """Parse MQTT topic into (machine_id, sensor_id).

    Expected format: <prefix>/<machine_id>/<sensor_id>
    Example: factory/machine_1/temperature -> ('machine_1', 'temperature')
    """
    parts = topic.strip("/").split("/")
    if len(parts) != 3 or parts[0] != prefix:
        return None
    return parts[1], parts[2]


def validate_payload(data: Any) -> bool:
    """Validate that the incoming JSON payload conforms to expected schema.

    Required fields:
        - machine_id (str)
        - sensor_id (str)
        - value (int or float)
        - timestamp (str ISO format or unix float)
    """
    if not isinstance(data, dict):
        return False

    required = ("machine_id", "sensor_id", "value", "timestamp")
    if not all(field in data for field in required):
        return False

    # Value must be numeric (not boolean, as bool is a subclass of int in Python)
    val = data["value"]
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        return False

    # Timestamp must be non-empty string or positive number
    ts = data["timestamp"]
    if not (isinstance(ts, str) and len(ts) > 0) and not (isinstance(ts, (int, float)) and ts > 0):
        return False

    return True


def parse_timestamp(ts_value: Any) -> datetime:
    """Parse string ISO or float timestamp into timezone-aware datetime."""
    if isinstance(ts_value, (int, float)):
        return datetime.fromtimestamp(ts_value, tz=timezone.utc)
    if isinstance(ts_value, str):
        try:
            # Handle standard ISO formats, including with Z suffix
            normalized = ts_value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class TelemetryIngestor:
    """Orchestrates MQTT ingestion, per-stream anomaly detection, and InfluxDB writes."""

    def __init__(self):
        # Dedicated anomaly detector for each distinct stream: (machine_id, sensor_id)
        self.detectors: Dict[Tuple[str, str], BaseDetector] = {}

        # InfluxDB client and synchronous write API
        self.influx_client: Optional[influxdb_client.InfluxDBClient] = None
        self.write_api = None
        self.mqtt_client: Optional[mqtt.Client] = None

        self.running = True
        self.messages_processed = 0
        self.anomalies_detected = 0

    def get_detector(self, machine_id: str, sensor_id: str) -> BaseDetector:
        """Retrieve or lazily instantiate the detector for a specific sensor stream."""
        key = (machine_id, sensor_id)
        if key not in self.detectors:
            logger.info(
                "Initializing %s detector for stream %s/%s",
                DETECTOR_TYPE,
                machine_id,
                sensor_id,
            )
            self.detectors[key] = create_detector(
                detector_type=DETECTOR_TYPE,
                window_size=ZSCORE_WINDOW_SIZE,
                threshold=ZSCORE_THRESHOLD,
                warmup_samples=IFOREST_WARMUP_SAMPLES,
                contamination=IFOREST_CONTAMINATION,
            )
        return self.detectors[key]

    def connect_influxdb(self) -> None:
        """Establish connection with InfluxDB 2.x and initialize write API."""
        logger.info("Connecting to InfluxDB at %s (org: %s, bucket: %s)...", INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET)
        self.influx_client = influxdb_client.InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
            timeout=10000,
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

    def process_message(self, topic: str, raw_payload: bytes) -> Optional[influxdb_client.Point]:
        """Parse, validate, detect anomalies, and build InfluxDB Point from raw message."""
        # 1. Parse topic
        topic_info = parse_topic(topic, MQTT_TOPIC_PREFIX)
        if not topic_info:
            logger.debug("Ignoring non-telemetry topic: %s", topic)
            return None
        topic_machine, topic_sensor = topic_info

        # 2. Decode and parse JSON
        try:
            payload = json.loads(raw_payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as err:
            logger.warning("Failed to decode JSON payload on %s: %s", topic, err)
            return None

        # 3. Validate schema
        if not validate_payload(payload):
            logger.warning("Invalid schema in payload on %s: %s", topic, payload)
            return None

        machine_id = payload.get("machine_id", topic_machine)
        sensor_id = payload.get("sensor_id", topic_sensor)
        value = float(payload["value"])
        timestamp = parse_timestamp(payload["timestamp"])
        unit = str(payload.get("unit", ""))

        # 4. Execute Anomaly Detection
        detector = self.get_detector(machine_id, sensor_id)
        is_anomaly, score = detector.detect(value)

        self.messages_processed += 1
        if is_anomaly:
            self.anomalies_detected += 1
            logger.warning(
                "ANOMALY DETECTED on %s/%s: value=%.2f %s | score=%.3f | type=%s",
                machine_id,
                sensor_id,
                value,
                unit,
                score,
                DETECTOR_TYPE,
            )

        # 5. Build InfluxDB Point
        # measurement: sensor_telemetry
        # tags: machine_id, sensor_id, detector
        # fields: value (float), is_anomaly (int 0 or 1), anomaly_score (float), unit (str)
        point = (
            influxdb_client.Point("sensor_telemetry")
            .tag("machine_id", machine_id)
            .tag("sensor_id", sensor_id)
            .tag("detector", DETECTOR_TYPE)
            .field("value", float(value))
            .field("is_anomaly", 1 if is_anomaly else 0)
            .field("anomaly_score", float(score))
            .field("unit", unit)
            .time(timestamp)
        )
        return point

    def on_mqtt_message(self, client, userdata, message):
        """Callback executed whenever a telemetry MQTT message is received."""
        try:
            point = self.process_message(message.topic, message.payload)
            if point and self.write_api:
                self.write_api.write(
                    bucket=INFLUXDB_BUCKET,
                    org=INFLUXDB_ORG,
                    record=point,
                )
        except Exception as err:
            logger.error("Error ingesting message from %s: %s", message.topic, err, exc_info=True)

    def start(self) -> None:
        """Start the ingestor pipeline."""
        logger.info("Initializing IoT Ingestor Service...")
        logger.info("Detector configured: %s", DETECTOR_TYPE)

        # Connect to InfluxDB
        self.connect_influxdb()

        # Build MQTT client
        client_id = f"iot_ingestor_{os.getpid()}"
        if hasattr(mqtt, "CallbackAPIVersion"):
            self.mqtt_client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
            )
        else:
            self.mqtt_client = mqtt.Client(client_id=client_id)

        subscription_topic = f"{MQTT_TOPIC_PREFIX}/#"

        def on_connect(client, userdata, flags, rc, *args):
            code = getattr(rc, "value", rc)
            if code == 0:
                logger.info("Connected to MQTT broker. Subscribing to: %s", subscription_topic)
                client.subscribe(subscription_topic, qos=1)
            else:
                logger.error("Failed to connect to MQTT broker (rc: %s)", rc)

        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_message = self.on_mqtt_message

        # Signal handling
        def handle_exit(signum, frame):
            logger.info("Exit signal received (%s). Shutting down...", signum)
            self.running = False

        signal.signal(signal.SIGINT, handle_exit)
        signal.signal(signal.SIGTERM, handle_exit)

        # Connect with retry logic
        connected = False
        while self.running and not connected:
            try:
                logger.info("Connecting to MQTT broker at %s:%d...", MQTT_BROKER, MQTT_PORT)
                self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                self.mqtt_client.loop_start()
                connected = True
            except Exception as e:
                logger.warning("MQTT broker unavailable (%s). Retrying in 2 seconds...", e)
                time.sleep(2.0)

        # Keep alive loop
        try:
            while self.running:
                time.sleep(1.0)
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully release MQTT and InfluxDB resources."""
        logger.info("Stopping Ingestor service...")
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        if self.write_api:
            self.write_api.close()
        if self.influx_client:
            self.influx_client.close()
        logger.info(
            "Ingestor stopped. Total processed: %d, Anomalies: %d",
            self.messages_processed,
            self.anomalies_detected,
        )


def main():
    ingestor = TelemetryIngestor()
    ingestor.start()


if __name__ == "__main__":
    main()
