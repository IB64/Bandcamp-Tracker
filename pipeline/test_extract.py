import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock
from extract import (
    unix_time_millis,
    load_sales_data,
    get_tags_from_url,
    get_title_from_url,
    extract_data_from_json
)


class TestBandcampAPI(unittest.TestCase):

    def test_unix_time_millis(self):
        dt = datetime(2023, 1, 1)
        result = unix_time_millis(dt)
        expected = 1672531200000.0
        self.assertEqual(result, expected)

    @patch("extract.requests.get")
    def test_load_sales_data(self, mock_get):
        mock_response = MagicMock()

        mock_response.json.return_value = {"events": []}
        mock_get.return_value = mock_response

        dt = datetime.utcnow()
        result = load_sales_data(dt)
        self.assertEqual(result, {"events": []})

    def test_get_tags_from_url(self):
        html = '<a class="tag">rock</a>'
        result = get_tags_from_url(html)
        self.assertEqual(result, ["rock"])

    def test_get_title_from_url(self):
        html = '<h2 class="trackTitle">Sample Title</h2>'
        result = get_title_from_url(html)
        self.assertEqual(result, "Sample Title")

    @patch("extract.get_html")
    @patch("extract.get_tags_from_url")
    @patch("extract.get_title_from_url")
    def test_extract_data_from_json(self, mock_get_title, mock_get_tags, mock_get_html):
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
                            "url": "https://example.com"
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
            "at": datetime.utcfromtimestamp(1641100800)
        }]
        self.assertEqual(result, expected)
