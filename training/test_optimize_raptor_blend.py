"""Tests for training-only RAPTOR component-weight optimization."""

import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from optimize_raptor_blend import fit_weights


class OptimizeRaptorBlendTests(unittest.TestCase):
    def test_recovers_no_intercept_weights(self):
        X = np.array([[1.0, 0.0], [0.0, 1.0], [2.0, -1.0], [3.0, 4.0]])
        expected = np.array([0.85, 0.21])
        np.testing.assert_allclose(fit_weights(X, X @ expected), expected,
                                   atol=1e-12)

    def test_rejects_bad_shape(self):
        with self.assertRaises(ValueError):
            fit_weights(np.ones((3, 3)), np.ones(3))


if __name__ == "__main__":
    unittest.main()
