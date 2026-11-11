"""Unit tests for Anomaly Detection algorithms (Z-Score and Isolation Forest)."""

import numpy as np
import pytest

from detectors import (
    IsolationForestDetector,
    ZScoreDetector,
    create_detector,
)


class TestZScoreDetector:
    """Test suite for the rolling Z-Score anomaly detector."""

    def test_zscore_warmup_phase(self):
        """Values during initial window fill (min_samples) should not trigger anomalies."""
        detector = ZScoreDetector(window_size=20, threshold=3.0, min_samples=5)
        for val in [10.0, 10.5, 9.8, 10.2]:
            is_anomaly, score = detector.detect(val)
            assert not is_anomaly
            assert score == 0.0

    def test_zscore_normal_distribution(self):
        """Stable series with small variance should not be flagged as anomalous."""
        detector = ZScoreDetector(window_size=30, threshold=3.0, min_samples=5)
        rng = np.random.default_rng(seed=42)
        normal_data = rng.normal(loc=50.0, scale=1.0, size=50)

        anomalies = []
        for val in normal_data:
            is_anomaly, _ = detector.detect(float(val))
            anomalies.append(is_anomaly)

        # In a standard normal distribution with 3 sigma threshold, no anomalies expected in 50 samples
        assert sum(anomalies) == 0

    def test_zscore_detects_positive_spike(self):
        """A sudden extreme positive jump must be detected."""
        detector = ZScoreDetector(window_size=20, threshold=3.0, min_samples=5)
        # Establish stable baseline
        for _ in range(15):
            detector.detect(25.0)

        # Extreme outlier
        is_anomaly, score = detector.detect(100.0)
        assert is_anomaly is True
        assert score >= 3.0

    def test_zscore_detects_negative_spike(self):
        """A sudden extreme drop must be detected."""
        detector = ZScoreDetector(window_size=20, threshold=3.0, min_samples=5)
        for _ in range(15):
            detector.detect(100.0)

        is_anomaly, score = detector.detect(10.0)
        assert is_anomaly is True
        assert score >= 3.0

    def test_zscore_zero_variance_flatline(self):
        """Handles edge-case where all recent values are identical (std = 0)."""
        detector = ZScoreDetector(window_size=10, threshold=3.0, min_samples=5)
        for _ in range(8):
            is_anomaly, _ = detector.detect(42.0)
            assert not is_anomaly

        # Continuing flatline should not flag
        is_anomaly, score = detector.detect(42.0)
        assert not is_anomaly
        assert score == 0.0

        # Departure from zero-variance baseline must flag
        is_anomaly, score = detector.detect(60.0)
        assert is_anomaly is True
        assert score == 999.0

    def test_zscore_reset(self):
        """Reset clears window state."""
        detector = ZScoreDetector(window_size=10)
        for _ in range(10):
            detector.detect(50.0)
        assert len(detector.window) == 10
        detector.reset()
        assert len(detector.window) == 0


class TestIsolationForestDetector:
    """Test suite for Scikit-Learn IsolationForest anomaly detector."""

    def test_isolation_forest_warmup(self):
        """Model should collect warmup_samples before activating."""
        detector = IsolationForestDetector(warmup_samples=30, contamination=0.05)
        for i in range(29):
            is_anomaly, score = detector.detect(20.0 + (i % 3) * 0.1)
            assert not is_anomaly
            assert score == 0.0
        assert not detector.is_fitted

        # 30th sample triggers fit
        detector.detect(20.2)
        assert detector.is_fitted

    def test_isolation_forest_detects_outlier(self):
        """After training on normal data, detects prominent outliers."""
        detector = IsolationForestDetector(warmup_samples=50, contamination=0.05, random_state=42)
        rng = np.random.default_rng(seed=42)

        # 50 normal observations around 60.0 with std 1.0
        for _ in range(50):
            val = float(rng.normal(60.0, 1.0))
            detector.detect(val)

        assert detector.is_fitted

        # Normal point close to mean
        is_anomaly_normal, score_normal = detector.detect(60.2)
        assert not is_anomaly_normal

        # Extreme outlier far outside distribution
        is_anomaly_outlier, score_outlier = detector.detect(250.0)
        assert is_anomaly_outlier is True
        assert score_outlier > score_normal

    def test_isolation_forest_pre_fit(self):
        """Direct fit with historical baseline data."""
        detector = IsolationForestDetector(contamination=0.05, random_state=42)
        baseline = list(np.random.normal(30.0, 0.5, size=100))
        detector.fit(baseline)
        assert detector.is_fitted

        is_anomaly, _ = detector.detect(30.1)
        assert not is_anomaly

        is_anomaly, _ = detector.detect(180.0)
        assert is_anomaly is True


class TestDetectorFactory:
    """Tests for create_detector factory function."""

    def test_create_zscore(self):
        det = create_detector("zscore", window_size=25, threshold=2.5)
        assert isinstance(det, ZScoreDetector)
        assert det.window_size == 25
        assert det.threshold == 2.5

    def test_create_isolation_forest(self):
        det = create_detector("isolation_forest", warmup_samples=50, contamination=0.08)
        assert isinstance(det, IsolationForestDetector)
        assert det.warmup_samples == 50
        assert det.contamination == 0.08

    def test_create_unknown_detector_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported detector type"):
            create_detector("unsupported_algo")
