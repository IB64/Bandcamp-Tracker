"""
Tests the functions within extract.py script 
"""

from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from requests.exceptions import Timeout, HTTPError

from extract import (
    get_minute_rounded_down,
    unix_time_seconds,
    load_sales_data,
    get_tags_from_url,
    get_title_from_url,
    extract_data_from_json
)

EXAMPLE_DATETIME = datetime(2023, 1, 1)
EXAMPLE_UNIX_TIME = 1672531200


class TestBandcampAPI:
    """
    Class used for testing base cases
    """

    def test_unix_time_seconds(self):
        """
        Test whether the function returns the correct value
        """
        result = unix_time_seconds(EXAMPLE_DATETIME)
        assert result == EXAMPLE_UNIX_TIME
        assert isinstance(result, int) is True

    @patch("extract.requests.get")
    def test_load_sales_data(self, mock_get):
        """
        Tests whether the appropriate sales data are returned - base cases
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {"events": []}
        mock_get.return_value = mock_response
        result = load_sales_data(EXAMPLE_DATETIME)
        assert result == {"events": []}

    def test_get_tags_from_url(self):
        """
        Test whether appropriate tags are extracted from html
        """
        html = '<a class="tag">rock</a>'
        result = get_tags_from_url(html)
        assert result == ["rock"]

    def test_get_title_from_url(self):
        """
        Test whether the title of an album or track is found
        """
        html = '<h2 class="trackTitle">Sample Title</h2>'
        result = get_title_from_url(html)
        assert result == "Sample Title"

    @patch("extract.get_html")
    @patch("extract.get_tags_from_url")
    @patch("extract.get_title_from_url")
    def test_extract_data_from_json(self, mock_get_title, mock_get_tags, mock_get_html):
        """
        Test whether the appropriate data is extracted from a given JSON
        """
        mock_get_html.return_value = "<html></html>"
        mock_get_tags.return_value = ["rock"]
        mock_get_title.return_value = "Sample Title"

        sales_json = {
            "events": [
                {
                    "event_type": "sale",
                    "items": [
                        {
                            "amount_paid_usd": 10,
                            "country": "US",
                            "artist_name": "Artist",
                            "utc_date": 1641100800,
                            "item_type": "a",
                            "url": "https://example.com",
                            "art_url": "https://exampleimage.com"
                        }
                    ]
                }
            ]
        }

        result = extract_data_from_json(sales_json)
        expected = [{
            "amount_paid_usd": 10,
            "tags": ['rock'],
            "country": "US",
            "title": "Sample Title",
            "artist": "Artist",
            "at": datetime.utcfromtimestamp(1641100800).strftime("%m/%d/%Y, %H:%M:%S"),
            "type": "album",
            "image": "https://exampleimage.com"
        }]
        assert result == expected

    def test_get_minute_rounded_down(self):
        """
        Test for base cases for func get_minute_rounded_down
        """
        dt = datetime(2024, 1, 5, 12, 34, 56, 789000)
        expected_result = datetime(2024, 1, 5, 12, 34, 0, 0)
        assert get_minute_rounded_down(dt) == expected_result


class TestBandCampAPIFails:
    """
    Tests whether appropriate responses occur when an error occurs
    """

    @patch("extract.requests.get", side_effect=ConnectionError("Connection failed"))
    def test_connection_error(self, mock_get):
        """Test whether a ConnectionError is thrown"""
        with pytest.raises(ConnectionError):
            load_sales_data(EXAMPLE_DATETIME)

    @patch("extract.requests.get", side_effect=Timeout("The request timed out."))
    def test_timeout_error(self, mock_get):
        """Test whether a Timeout is thrown"""
        with pytest.raises(Timeout):
            load_sales_data(EXAMPLE_DATETIME)

    @patch("extract.requests.get", side_effect=HTTPError("url is invalid."))
    def test_http_error(self, mock_get):
        """Test whether a HTTPError is thrown"""
        with pytest.raises(HTTPError):
            load_sales_data(EXAMPLE_DATETIME)
