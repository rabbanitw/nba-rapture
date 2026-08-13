"""Small deterministic tests for the published RAPTOR post-processing math."""

import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent / "raptor2"))

from postprocess import (combine_components, raptor_war, reconcile_team_rating,
                         score_effect, tied_game_rating)


class PostprocessTests(unittest.TestCase):
    def test_published_component_blend(self):
        self.assertAlmostEqual(float(combine_components(3.0, 3.0)), 3.18)

    def test_score_effects_and_tied_context(self):
        effects = score_effect([10, 10, 10, 10, 20], [1, 2, 3, 4, 5])
        np.testing.assert_allclose(effects, [-1.1, -1.7, -2.3, -2.9, -5.8])
        self.assertAlmostEqual(float(score_effect(-20, 3)), 4.6)
        self.assertAlmostEqual(float(score_effect(10, 4, playoffs=True)), -1.5)
        self.assertAlmostEqual(float(tied_game_rating(2.0, -1.5)), 3.5)

    def test_team_reconciliation_constraint(self):
        r = np.array([1.0, 0.0, -1.0])
        mp = np.array([100.0, 200.0, 100.0])
        usage = np.array([2.0, 1.0, 0.5])
        out = reconcile_team_rating(r, mp, usage, team_rating=4.0)
        self.assertAlmostEqual(5.0 * np.average(out, weights=mp), 4.0)
        self.assertGreater(out[0] - r[0], out[2] - r[2])

    def test_war_formula(self):
        expected = (5.0 + 2.75) * 2000 * (101.0 / 100.0) * 0.0005102
        self.assertAlmostEqual(float(raptor_war(5.0, 2000, 100, 1.0)), expected)


if __name__ == "__main__":
    unittest.main()
