"""Anomaly Detectors for Real-Time IoT Sensor Telemetry.

Provides:
- BaseDetector: Common abstract base class for detectors.
- ZScoreDetector: Lightweight statistical detector based on a rolling window.
- IsolationForestDetector: Machine-learning based detector using Scikit-Learn IsolationForest.
"""

from abc import ABC, abstractmethod
from collections import deque
from typing import Any, List, Optional, Tuple, Union

import numpy as np
from sklearn.ensemble import IsolationForest


class BaseDetector(ABC):
    """Abstract Base Class for streaming anomaly detectors."""

    @abstractmethod
    def fit(self, data: Union[List[float], np.ndarray]) -> None:
        """Train or initialize the detector with historical/baseline data."""
        pass

    @abstractmethod
    def detect(self, value: float) -> Tuple[bool, float]:
        """Evaluate a single new data point.

        Returns:
            Tuple[bool, float]:
                - is_anomaly (bool): True if the point is considered an anomaly.
                - score (float): Normalized anomaly metric (e.g., z-score or inverted decision score).
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset internal detector state."""
        pass


class ZScoreDetector(BaseDetector):
    """Streaming Z-Score Anomaly Detector.

    Uses a sliding window (FIFO deque) of recent values to calculate the rolling
    mean and standard deviation.

    Anomaly condition:
        z = |value - mean| / std
        is_anomaly = z > threshold
    """

    def __init__(
        self,
        window_size: int = 30,
        threshold: float = 3.0,
        min_samples: int = 5,
    ) -> None:
        """Initialize ZScoreDetector.

        Args:
            window_size: Maximum number of recent samples kept in the sliding window.
            threshold: Number of standard deviations defining an anomaly (typically 3.0).
            min_samples: Minimum observations required before evaluating anomalies.
        """
        self.window_size = int(window_size)
        self.threshold = float(threshold)
        self.min_samples = int(min_samples)
        self.window: deque = deque(maxlen=self.window_size)

    def fit(self, data: Union[List[float], np.ndarray]) -> None:
        """Pre-populate the sliding window with baseline data."""
        self.window.clear()
        for x in data[-self.window_size:]:
            self.window.append(float(x))

    def detect(self, value: float) -> Tuple[bool, float]:
        """Compute rolling Z-score for the given value and update window."""
        val = float(value)

        # Insufficient data to reliably estimate mean and variance
        if len(self.window) < self.min_samples:
            self.window.append(val)
            return False, 0.0

        arr = np.array(self.window, dtype=np.float64)
        mean = float(np.mean(arr))
        std = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0

        # Handle edge case: variance is zero or negligible (flatline / constant values)
        if std < 1e-6:
            diff = abs(val - mean)
            if diff > 1e-3:
                # Sudden departure from completely flat baseline
                z_score = 999.0
                is_anomaly = True
            else:
                z_score = 0.0
                is_anomaly = False
        else:
            z_score = abs(val - mean) / std
            is_anomaly = bool(z_score > self.threshold)

        # Append new value into sliding window
        self.window.append(val)
        return is_anomaly, round(float(z_score), 3)

    def reset(self) -> None:
        """Clear the sliding window."""
        self.window.clear()


class IsolationForestDetector(BaseDetector):
    """Machine Learning Anomaly Detector using Scikit-Learn IsolationForest.

    Operates in two phases:
    1. Warm-up phase: Collects the initial `warmup_samples` normal readings.
    2. Detection phase: Fits IsolationForest on baseline readings, then evaluates
       incoming stream values using decision_function.
    """

    def __init__(
        self,
        warmup_samples: int = 40,
        contamination: float = 0.05,
        random_state: int = 42,
    ) -> None:
        """Initialize IsolationForestDetector.

        Args:
            warmup_samples: Number of initial stream values used to train the model.
            contamination: Expected proportion of outliers in the data.
            random_state: Seed for reproducibility.
        """
        self.warmup_samples = int(warmup_samples)
        self.contamination = float(contamination)
        self.random_state = int(random_state)

        self.model: Optional[IsolationForest] = None
        self.warmup_buffer: List[float] = []
        self.is_fitted = False

    def fit(self, data: Union[List[float], np.ndarray]) -> None:
        """Fit IsolationForest on provided baseline data."""
        arr = np.array(data, dtype=np.float64).reshape(-1, 1)
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100,
        )
        self.model.fit(arr)
        self.is_fitted = True

    def detect(self, value: float) -> Tuple[bool, float]:
        """Detect if value is an outlier using the trained IsolationForest."""
        val = float(value)

        # Collect warmup readings if model is not yet trained
        if not self.is_fitted:
            self.warmup_buffer.append(val)
            if len(self.warmup_buffer) >= self.warmup_samples:
                self.fit(self.warmup_buffer)
                self.warmup_buffer.clear()
            return False, 0.0

        # Predict anomaly: -1 represents outlier, 1 represents inlier
        x = np.array([[val]], dtype=np.float64)
        pred = self.model.predict(x)[0]
        is_anomaly = bool(pred == -1)

        # decision_function: lower score = more abnormal
        # We invert the score so higher positive value = higher anomaly severity
        raw_score = float(self.model.decision_function(x)[0])
        anomaly_score = -raw_score

        return is_anomaly, round(anomaly_score, 4)

    def reset(self) -> None:
        """Reset internal buffer and model."""
        self.warmup_buffer.clear()
        self.model = None
        self.is_fitted = False


def create_detector(detector_type: str = "zscore", **kwargs: Any) -> BaseDetector:
    """Factory function to instantiate anomaly detectors.

    Args:
        detector_type: 'zscore' or 'isolation_forest'
        **kwargs: Detector-specific arguments.
    """
    normalized = detector_type.strip().lower()
    if normalized in ("zscore", "z_score"):
        return ZScoreDetector(
            window_size=kwargs.get("window_size", 30),
            threshold=kwargs.get("threshold", 3.0),
        )
    elif normalized in ("isolation_forest", "iforest"):
        return IsolationForestDetector(
            warmup_samples=kwargs.get("warmup_samples", 40),
            contamination=kwargs.get("contamination", 0.05),
        )
    else:
        raise ValueError(
            f"Unsupported detector type '{detector_type}'. Supported: 'zscore', 'isolation_forest'"
        )
