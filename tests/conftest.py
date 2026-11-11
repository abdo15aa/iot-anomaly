"""Pytest configuration and shared fixtures for IoT Anomaly Detection."""

import sys
from pathlib import Path

# Add project root, ingestor, and simulator directories to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "ingestor"))
sys.path.insert(0, str(PROJECT_ROOT / "simulator"))
