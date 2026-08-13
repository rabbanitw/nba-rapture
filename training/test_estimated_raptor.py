"""Regression tests for Estimated RAPTOR identity normalization."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from estimated_raptor import norm_name


class EstimatedRaptorNameTests(unittest.TestCase):
    def test_jr_as_first_name_is_preserved(self):
        self.assertEqual(norm_name("JR Smith"), "jrsmith")

    def test_generational_suffix_is_removed_only_at_end(self):
        self.assertEqual(norm_name("Gary Payton II"), "garypayton")
        self.assertEqual(norm_name("Tim Hardaway Jr."), "timhardaway")

    def test_diacritics_and_punctuation_normalize(self):
        self.assertEqual(norm_name("Nikola Jokić"), "nikolajokic")
        self.assertEqual(norm_name("Kentavious Caldwell-Pope"),
                         "kentaviouscaldwellpope")


if __name__ == "__main__":
    unittest.main()
