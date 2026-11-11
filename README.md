# 🏭 IoT Anomaly Detection & Industrial Surveillance Platform

[![CI Pipeline](https://github.com/user/iot-anomaly/actions/workflows/ci.yml/badge.svg)](https://github.com/user/iot-anomaly/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Docker Compose](https://img.shields.io/badge/docker--compose-v2-2496ED.svg?logo=docker)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An end-to-end industrial IoT monitoring and real-time streaming anomaly detection platform.
Built with clean architecture, containerized with Docker Compose, and designed for high observability and maintainability.

---

## 🏛️ System Architecture

```text
+-----------------------+           +----------------------+
|  Python IoT Simulator |           |   Eclipse Mosquitto  |
|  - 4 Sensors / Mach.  | --MQTT--> |   MQTT Message Broker|
|  - Noise & Anomalies  |  (QoS 1)  |   Port: 1883         |
+-----------------------+           +----------------------+
                                               |
                                            Subscribe
                                        (factory/#)
                                               v
+-----------------------+           +----------------------+
|     Grafana 10.x      |           | Python Stream Ingest |
|  - Auto-provisioned   | <--Flux-- | - Topic parser & Val |
|  - Real-time Curves   |   Query   | - Z-Score / IFOREST  |
|  - Anomaly Badges     |           | - Per-stream state   |
+-----------------------+           +----------------------+
            ^                                  |
            |                               Writes
            |                             Line Proto
            +------- [ InfluxDB 2.x ] <--------+
                     Time-Series DB
                     Port: 8086
```

---

## 📂 Repository Structure

```text
iot-anomaly/
├── docker-compose.yml              # Complete container orchestration
├── .env.example                    # Environment configuration template
├── .gitignore                      # Git exclusion rules
├── mosquitto/
│   └── mosquitto.conf              # Broker listener & persistence config
├── simulator/
│   ├── Dockerfile                  # Lightweight Python 3.11 image
│   ├── requirements.txt            # paho-mqtt
│   └── simulator.py                # Telemetry generation & anomaly injector
├── ingestor/
│   ├── Dockerfile                  # Scikit-learn & InfluxDB ingestor image
│   ├── requirements.txt            # paho-mqtt, influxdb-client, scikit-learn
│   ├── ingestor.py                 # MQTT consumer & InfluxDB writer
│   └── detectors.py                # ZScore & IsolationForest implementations
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── influxdb.yml        # Auto-configured InfluxDB Flux source
│       └── dashboards/
│           ├── dashboards.yml      # Dashboards provider definition
│           └── iot_anomaly_dashboard.json # Rich dashboard with alert markers
├── tests/
│   ├── conftest.py                 # Pytest path & fixture configuration
│   ├── test_detectors.py           # Z-Score & Isolation Forest unit tests
│   └── test_ingestor.py            # Topic parsing, JSON schema, and pipeline tests
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions (Lint, Pytest, Docker build)
├── docs/
│   └── EXPLAIN.md                  # Comprehensive technical & interview guide
└── README.md                       # Project documentation
```

---

## 🚀 Quick Start (Docker Compose)

### 1. Prerequisites
- [Docker Engine & Docker Compose v2+](https://docs.docker.com/get-docker/)
- Optional: Python 3.11+ for local unit test execution

### 2. Configure Environment
```bash
# Copy environment file
cp .env.example .env
```
*(Default values in `.env.example` work out-of-the-box for local testing.)*

### 3. Build & Start Services
```bash
docker compose up --build -d
```

### 4. Verify Service Status
```bash
docker compose ps
```

Expected output:
```text
NAME             IMAGE                    STATUS                  PORTS
iot_mosquitto    eclipse-mosquitto:2.0    Up (healthy)            0.0.0.0:1883->1883/tcp
iot_influxdb     influxdb:2.7-alpine      Up (healthy)            0.0.0.0:8086->8086/tcp
iot_grafana      grafana/grafana:10.4.0   Up                      0.0.0.0:3000->3000/tcp
iot_ingestor     iot_ingestor:latest      Up                      
iot_simulator    iot_simulator:latest     Up                      
```

### 5. Access Dashboards
- **Grafana UI**: [http://localhost:3000](http://localhost:3000)
  - **Login**: `admin` / `admin`
  - Navigate to **Dashboards** > **IoT Surveillance** > **IoT Factory Telemetry & Anomaly Surveillance**
- **InfluxDB 2.x UI**: [http://localhost:8086](http://localhost:8086)
  - **Login**: `admin` / `adminpassword123`
  - Organization: `iot_org` | Bucket: `sensor_telemetry`

---

## 📊 Live Screenshots

### 1. Real-Time Grafana Surveillance Dashboard
<!-- Place your screenshot here: assets/screenshots/grafana_dashboard.png -->
```text
+-------------------------------------------------------------------------------+
|  TOTAL ANOMALIES: 14    | ACTIVE MACHINES: 2    | PEAK SEVERITY SCORE: 4.82   |
+-------------------------------------------------------------------------------+
|  [ Temperature Curve (°C) ]               |  [ Vibration Curve (mm/s) ]       |
|  ~ ~ ~ ~ ~ ~ ~ ~ ~ * (Spike Detected!)    |  ~ ~ ~ ~ ~ * (High Vibration!)    |
+-------------------------------------------------------------------------------+
|  [ Pressure Curve (bar) ]                 |  [ Humidity Curve (%) ]           |
|  ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~          |  ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~    |
+-------------------------------------------------------------------------------+
```

### 2. Terminal Live Ingestion Logs
<!-- Place your screenshot here: assets/screenshots/terminal_logs.png -->
```text
2026-11-10 14:05:12 [INFO] [Ingestor] Initializing zscore detector for stream machine_1/temperature
2026-11-10 14:05:13 [WARNING] [Simulator] Injected SPIKE on machine_1/temperature: 104.20 celsius
2026-11-10 14:05:13 [WARNING] [Ingestor] ANOMALY DETECTED on machine_1/temperature: value=104.20 celsius | score=25.821 | type=zscore
```

---

## 🔬 Technical Choices & Engineering Rationale

| Layer | Component | Choice Rationale |
|---|---|---|
| **Message Broker** | **Eclipse Mosquitto (MQTT)** | Lightweight footprint (< 10 MB RAM), pub/sub decoupling, QoS 1 delivery guarantees, industrial standard (Industry 4.0 / SCADA). |
| **Time-Series Storage** | **InfluxDB 2.x** | Optimized specifically for high-throughput append-only time-stamped metrics. Efficient tag indexing (`machine_id`, `sensor_id`) and native Flux analytical engine. |
| **Statistical Detection** | **Z-Score (Rolling Window)** | $\mathcal{O}(1)$ time complexity per observation, zero offline training needed, ideal for immediate spikes, sensor failure flatlines, and abrupt regime changes. |
| **ML Detection** | **Isolation Forest** | Non-parametric unsupervised ensemble algorithm (`scikit-learn`). Isolates anomalies instead of modeling normal density, robust against non-linear and multi-modal distributions. |
| **Visualization** | **Grafana 10.x** | Declarative provisioning (datasources & dashboards fully codified in Git), live auto-refresh, high-contrast industrial visual components. |
| **Containerization** | **Docker Compose v2** | Zero host dependency installation, reproducible builds, explicit inter-service healthcheck dependencies. |

---

## 🧪 Unit Testing & Quality Checks

Run the complete test suite locally:

```bash
# Install development dependencies
pip install -r simulator/requirements.txt -r ingestor/requirements.txt pytest ruff

# Run code style linter
ruff check .

# Run pytest unit tests with verbose output
pytest -v
```

---

## 🛠️ Stopping the Platform

```bash
# Stop all containers
docker compose down

# Stop and purge volumes (wipes data for a clean restart)
docker compose down -v
```
