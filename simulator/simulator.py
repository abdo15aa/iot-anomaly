"""IoT Sensor Telemetry Simulator.

Simulates industrial machine sensors (temperature, vibration, pressure, humidity)
with Gaussian baseline noise and injects controlled anomalies (spikes, drift, stuck values).
Publishes telemetry messages in JSON format over MQTT.
"""

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict

import paho.mqtt.client as mqtt

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Simulator] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("simulator")

# ------------------------------------------------------------------------------
# Configuration loaded from environment variables
# ------------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "factory")
PUBLISH_INTERVAL_SEC = float(os.getenv("SIMULATOR_PUBLISH_INTERVAL_SEC", "1.0"))
ANOMALY_RATE = float(os.getenv("SIMULATOR_ANOMALY_RATE", "0.05"))
MACHINES = [m.strip() for m in os.getenv("SIMULATOR_MACHINES", "machine_1,machine_2").split(",") if m.strip()]

# ------------------------------------------------------------------------------
# Sensor Profiles: baseline values, standard deviations, and physical units
# ------------------------------------------------------------------------------
SENSOR_PROFILES: Dict[str, Dict[str, Any]] = {
    "temperature": {
        "mean": 65.0,
        "std": 1.5,
        "unit": "celsius",
        "spike_magnitude": 35.0,
        "drift_step": 2.0,
    },
    "vibration": {
        "mean": 2.5,
        "std": 0.3,
        "unit": "mm/s",
        "spike_magnitude": 8.0,
        "drift_step": 0.5,
    },
    "pressure": {
        "mean": 5.0,
        "std": 0.2,
        "unit": "bar",
        "spike_magnitude": 4.0,
        "drift_step": 0.4,
    },
    "humidity": {
        "mean": 45.0,
        "std": 2.0,
        "unit": "%",
        "spike_magnitude": 30.0,
        "drift_step": 2.5,
    },
}


class SensorSimulator:
    """Manages sensor state per machine and generates realistic telemetry with anomalies."""

    def __init__(self, machines: list[str], anomaly_rate: float = 0.05):
        self.machines = machines
        self.anomaly_rate = anomaly_rate

        # Active anomaly state per (machine, sensor)
        # Format: {"type": "drift"|"stuck", "duration": int, "value": float, "step": float}
        self.anomaly_states: Dict[tuple[str, str], Dict[str, Any]] = {}

    def generate_reading(self, machine_id: str, sensor_id: str) -> tuple[float, bool]:
        """Generate a single sensor reading.

        Returns:
            Tuple of (sensor_value, is_injected_anomaly)
        """
        profile = SENSOR_PROFILES[sensor_id]
        key = (machine_id, sensor_id)
        is_anomaly = False

        # 1. Continue ongoing multi-step anomaly if active
        if key in self.anomaly_states:
            state = self.anomaly_states[key]
            state["duration"] -= 1

            if state["type"] == "stuck":
                val = state["value"]
            elif state["type"] == "drift":
                state["value"] += state["step"]
                val = state["value"]
            else:
                val = profile["mean"]

            is_anomaly = True
            if state["duration"] <= 0:
                del self.anomaly_states[key]

            return round(val, 2), is_anomaly

        # 2. Check if a new anomaly should be triggered
        if random.random() < self.anomaly_rate:
            anomaly_type = random.choice(["spike", "drift", "stuck"])
            is_anomaly = True

            if anomaly_type == "spike":
                # Instant jump up or down by several standard deviations
                sign = random.choice([-1, 1])
                val = profile["mean"] + sign * profile["spike_magnitude"]
                logger.warning(
                    "Injected SPIKE on %s/%s: %.2f %s",
                    machine_id,
                    sensor_id,
                    val,
                    profile["unit"],
                )
                return round(val, 2), is_anomaly

            elif anomaly_type == "drift":
                # Starts a gradual drift lasting 5 to 10 iterations
                sign = random.choice([-1, 1])
                duration = random.randint(5, 10)
                step = sign * profile["drift_step"]
                initial_val = profile["mean"] + step
                self.anomaly_states[key] = {
                    "type": "drift",
                    "duration": duration,
                    "value": initial_val,
                    "step": step,
                }
                logger.warning(
                    "Injected DRIFT on %s/%s for %d ticks",
                    machine_id,
                    sensor_id,
                    duration,
                )
                return round(initial_val, 2), is_anomaly

            elif anomaly_type == "stuck":
                # Sensor gets stuck at an arbitrary fixed value for 5 to 8 ticks
                duration = random.randint(5, 8)
                stuck_val = profile["mean"] + random.uniform(-1.0, 1.0) * profile["std"]
                self.anomaly_states[key] = {
                    "type": "stuck",
                    "duration": duration,
                    "value": stuck_val,
                    "step": 0.0,
                }
                logger.warning(
                    "Injected STUCK value on %s/%s: %.2f for %d ticks",
                    machine_id,
                    sensor_id,
                    stuck_val,
                    duration,
                )
                return round(stuck_val, 2), is_anomaly

        # 3. Normal behavior: Gaussian baseline noise
        normal_val = random.gauss(profile["mean"], profile["std"])
        return round(normal_val, 2), False


def build_mqtt_client() -> mqtt.Client:
    """Create and configure MQTT client supporting paho-mqtt v1 and v2 APIs."""
    client_id = f"iot_simulator_{os.getpid()}"

    # Use CallbackAPIVersion if supported (paho-mqtt 2.x)
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
    else:
        client = mqtt.Client(client_id=client_id)

    def on_connect(client, userdata, flags, rc, *args):
        # Support both v1 (rc is int) and v2 (rc is ReasonCode)
        code = getattr(rc, "value", rc)
        if code == 0:
            logger.info("Connected successfully to MQTT broker at %s:%d", MQTT_BROKER, MQTT_PORT)
        else:
            logger.error("Failed to connect to MQTT broker, return code: %s", rc)

    def on_disconnect(client, userdata, rc, *args):
        logger.warning("Disconnected from MQTT broker (rc: %s)", rc)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    return client


def main():
    """Main loop for the telemetry simulator."""
    logger.info("Starting IoT Telemetry Simulator...")
    logger.info("Machines: %s", MACHINES)
    logger.info("Sensors: %s", list(SENSOR_PROFILES.keys()))
    logger.info("Publish interval: %.2fs | Anomaly rate: %.2f", PUBLISH_INTERVAL_SEC, ANOMALY_RATE)

    simulator = SensorSimulator(machines=MACHINES, anomaly_rate=ANOMALY_RATE)
    client = build_mqtt_client()

    running = True

    def handle_shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received (%s). Exiting gracefully...", signum)
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Reconnect loop to wait for Mosquitto availability
    connected = False
    while running and not connected:
        try:
            logger.info("Attempting connection to MQTT broker %s:%d...", MQTT_BROKER, MQTT_PORT)
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_start()
            connected = True
        except Exception as e:
            logger.warning("Broker unavailable (%s). Retrying in 2 seconds...", e)
            time.sleep(2.0)

    try:
        while running:
            loop_start = time.time()

            for machine_id in MACHINES:
                for sensor_id, profile in SENSOR_PROFILES.items():
                    value, injected = simulator.generate_reading(machine_id, sensor_id)

                    # Build telemetry payload
                    payload = {
                        "machine_id": machine_id,
                        "sensor_id": sensor_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "value": value,
                        "unit": profile["unit"],
                        "_injected_anomaly": injected,  # Informational tag for simulation validation
                    }

                    # Topic format: factory/<machine_id>/<sensor_id>
                    topic = f"{MQTT_TOPIC_PREFIX}/{machine_id}/{sensor_id}"
                    msg_str = json.dumps(payload)

                    client.publish(topic, msg_str, qos=1)

            # Sleep to maintain regular interval
            elapsed = time.time() - loop_start
            sleep_time = max(0.0, PUBLISH_INTERVAL_SEC - elapsed)
            time.sleep(sleep_time)

    finally:
        logger.info("Stopping MQTT client loop...")
        client.loop_stop()
        client.disconnect()
        logger.info("Simulator terminated cleanly.")


if __name__ == "__main__":
    main()
