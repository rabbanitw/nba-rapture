"""Synthetic invariants for the possession-level courtmate chain."""

import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent / "raptor2"))

from courtmate_chain import courtmate_chain_features


class CourtmateChainTests(unittest.TestCase):
    def test_own_ratings_and_sign(self):
        # Two ten-player groups alternate; every player has both shared and
        # apart possessions, which makes all three chain levels identifiable.
        off = np.array([
            [1, 2, 3, 4, 5], [1, 2, 3, 6, 7],
            [1, 4, 5, 6, 7], [2, 3, 4, 5, 6],
        ])
        deff = np.array([
            [11, 12, 13, 14, 15], [11, 12, 13, 16, 17],
            [11, 14, 15, 16, 17], [12, 13, 14, 15, 16],
        ])
        pts = np.array([1.0, 2.0, 3.0, 4.0])
        got = courtmate_chain_features(off, deff, pts).set_index("player_id")
        self.assertAlmostEqual(got.loc[1, "off_on"], 200.0)
        self.assertAlmostEqual(got.loc[11, "def_on"], -200.0)
        self.assertTrue(np.isfinite(got.loc[1, "off_courtmates_without"]))
        self.assertTrue(np.isfinite(got.loc[11, "def_courtmates_courtmates"]))

    def test_shape_validation(self):
        with self.assertRaises(ValueError):
            courtmate_chain_features(np.ones((2, 4)), np.ones((2, 4)), [1, 2])


if __name__ == "__main__":
    unittest.main()
