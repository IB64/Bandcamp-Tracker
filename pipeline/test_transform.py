"""
Script to test the functions within transform.py
"""
from collections import Counter

import pandas as pd

from transform import (
    convert_to_df,
    clean_tags,
    clean_artists,
    clean_dataframe,
    has_special_characters
)


class TestTransform:
    """
    Tests whether base cases results in expected outputs.
    """

    def test_convert_to_df(self):
        """
        Test whether correct data is converted into a dataframe
        """
        data = [{"tags": ["rock", "pop"], "title": "Song1",
                "amount_paid_usd": 10, "artist": "Artist1"},
                {"tags": ["jazz", "blues"], "title": "Song2",
                "amount_paid_usd": 20, "artist": "Artist2"}]
        result = convert_to_df(data)

        assert isinstance(result, pd.DataFrame) is True

    def test_clean_tags(self):
        """
        Tests tag cleaning
        """
        assert Counter(clean_tags(["Drum & Bass", "jazz"])) == Counter(
            ["DNB", "Jazz"])
        assert Counter(clean_tags(["Rnb", "rock"])) == Counter(["R&B", "Rock"])
        assert Counter(clean_tags(["John-Doe", "jazz"])) == Counter(["Jazz"])

    def test_clean_artists(self):
        """
        Test whether featured artist removal works
        """
        assert clean_artists("Artist1 ft. Artist2") == "Artist1"
        assert clean_artists("Bob featuring Bob2") == "Bob"
        assert clean_artists("Just artist") == "Just artist"

    def test_clean_dataframe(self):
        """
        Test whether cleaning produces expected outcomes
        """
        data = {"tags": [["rock"], ["pop", "jazz"]], "title": ["Song1", "Song2"],
                "amount_paid_usd": [10, 20], "artist": ["Artist1 ft. Artist2", "Artist2"]}
        df = pd.DataFrame(data)

        cleaned_df = clean_dataframe(df, True)

        assert "tags" in cleaned_df.columns
        assert "title" in cleaned_df.columns
        assert "amount_paid_usd" in cleaned_df.columns
        assert "artist" in cleaned_df.columns
        assert cleaned_df["amount_paid_usd"].dtype == "int64"

    def test_special_characters(self):
        """
        Test function "has_special_characters" with base cases
        """
        assert has_special_characters("a") is False
        assert has_special_characters("þ") is False
        assert has_special_characters("漢字") is True
        assert has_special_characters("a字") is True


class TestTransformErrors:
    """
    Tests for edge cases
    """

    def test_no_tags(self):
        """
        Test whether clean_tags() returns ["Other"] if no tags or a single empty tag are given
        """
        assert clean_tags([]) == ["Other"]
        assert clean_tags([""]) == ["Other"]

    def test_tag_empty_string(self):
        """
        Test whether empty strings are ignored when cleaning tags
        """
        assert Counter(clean_tags(["rock", ""])) == Counter(["Rock"])

    def test_has_special_characters_empty_string(self):
        """
        Test whether correct value is returned by func "has_special_character" with empty string
        """
        assert has_special_characters("") is False
