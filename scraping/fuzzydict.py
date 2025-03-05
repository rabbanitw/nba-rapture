from thefuzz import process


class FuzzyDict:
    def __init__(self, threshold=80):
        """
        Initialize the fuzzy dictionary.

        :param threshold: Minimum similarity score (0-100) for a match to be considered valid.
        """
        self.data = {}
        self.threshold = threshold

    def __setitem__(self, key, value):
        """Set a key-value pair."""
        self.data[key] = value

    def __getitem__(self, key):
        """Get a value with an exact or fuzzy-matched key."""
        if key in self.data:
            return self.data[key]

        best_match, score = process.extractOne(key, self.data.keys())
        if score >= self.threshold:
            return self.data[best_match]
        else:
            raise KeyError(f"No sufficiently close match found for '{key}'")

    def get(self, key, default=None):
        """Get a value with fuzzy matching, or return a default if no match is found."""
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        """Check if a key (or a close match) exists in the dictionary."""
        if key in self.data:
            return True
        best_match, score = process.extractOne(key, self.data.keys())
        return score >= self.threshold

    def update(self, dictionary):
        """Populate the fuzzy dictionary with a vanilla dictionary."""
        self.data.update(dictionary)

    def __repr__(self):
        """Return a string representation of the dictionary."""
        return f"FuzzyDict({self.data})"

