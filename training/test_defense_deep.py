"""Deterministic tests for defense ranking helpers."""

import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from experiment_defense_deep import (relevance_labels, remap_rank_distribution,
                                     season_top_weights)


class DefenseDeepTests(unittest.TestCase):
    def test_relevance_is_season_local_and_monotone(self):
        y = np.array([3.0, 2.0, 1.0, -5.0, -6.0, -7.0])
        seasons = np.array(["a", "a", "a", "b", "b", "b"])
        labels = relevance_labels(y, seasons, levels=10)
        np.testing.assert_array_equal(labels, [10, 5, 0, 10, 5, 0])

    def test_top_weights_emphasize_each_seasons_best_player(self):
        y = np.array([1.0, 3.0, 2.0, 4.0])
        seasons = np.array(["a", "a", "b", "b"])
        weights = season_top_weights(y, seasons, strength=2.0, decay=1.0)
        self.assertGreater(weights[1], weights[0])
        self.assertGreater(weights[3], weights[2])

    def test_rank_remap_preserves_baseline_distribution(self):
        base = np.array([1.0, 3.0, 2.0, -2.0, -1.0])
        ranker = np.array([3.0, 1.0, 2.0, -1.0, -2.0])
        seasons = np.array(["a", "a", "a", "b", "b"])
        out = remap_rank_distribution(base, ranker, seasons, alpha=0.5)
        for season in ("a", "b"):
            m = seasons == season
            np.testing.assert_array_equal(np.sort(out[m]), np.sort(base[m]))


if __name__ == "__main__":
    unittest.main()
