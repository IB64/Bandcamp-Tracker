"""
Script to interact with the Bandcamp API and extract relevant information
"""

from datetime import datetime
from urllib.request import urlopen

from bs4 import BeautifulSoup
import requests
from requests.exceptions import Timeout, HTTPError

EPOCH = datetime.utcfromtimestamp(0)
TIMEOUT = 20
ALBUM = "a"
TRACK = "t"
FIVE_MINS_IN_SECONDS = 300


def unix_time_seconds(dt: datetime) -> int:
    """
    Given a datetime, return the time in seconds since epoch as an int.
    """
    return int((dt - EPOCH).total_seconds())


def get_datetime_from_unix(time: float) -> str:
    """
    Given a unix time, convert that time into a datetime string.
    """
    return datetime.utcfromtimestamp(time).strftime("%m/%d/%Y, %H:%M:%S")


def get_minute_rounded_down(dt: datetime) -> datetime:
    """
    Round a datetime down to the nearest minute by setting seconds and microseconds to zero.
    """
    return dt.replace(second=0, microsecond=0)


def load_sales_data(dt: datetime) -> dict:
    """
    Uses the bandcamp API to return all the sales data from the last 5 minute in json format.
    """
    minute = get_minute_rounded_down(dt)
    seconds = unix_time_seconds(minute) - FIVE_MINS_IN_SECONDS
    try:
        response = requests.get(
            f"https://bandcamp.com/api/salesfeed/1/get?start_date={seconds}", timeout=TIMEOUT)
    except ConnectionError as exc:
        raise ConnectionError("Connection failed") from exc
    except Timeout as exc:
        raise Timeout("The request timed out.") from exc
    except HTTPError as exc:
        raise HTTPError("url is invalid.") from exc

    return response.json()


def get_html(url: str) -> str:
    """
    Given a url for a track, returns the associated html for that page.
    """
    with urlopen(url) as page:
        html_bytes = page.read()
        html_doc = html_bytes.decode("utf_8")
        return html_doc


def get_tags_from_url(html: str) -> list[str]:
    """
    Given the track page html, returns the associated tags for that track.
    """
    tags = []

    soup = BeautifulSoup(html, "lxml")

    for tag in soup.find_all("a", {"class": "tag"}):
        tags.append(tag.text)

    return tags


def get_title_from_url(html: str) -> str:
    """
    Given the track page html, returns the associated title for the track / album
    """

    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h2", {"class": "trackTitle"})

    return title.text


def extract_data_from_json(sales_json: dict) -> list[dict]:
    """
    Given the JSON response from a get request to the Bandcamp API,
    return a list of dicts with wanted information for each sale.
    """
    data = []

    events = sales_json["events"]
    for event in events:
        if event["event_type"] == "sale":
            items = event["items"]
            for item in items:
                # determine item type. Skip if not an album or track
                item_type = item["item_type"]
                if item_type not in (ALBUM, TRACK):
                    continue
                if item_type == ALBUM:
                    item_type = "album"
                elif item_type == TRACK:
                    item_type = "track"

                # append "https:" to urls that don't have it
                url = item["url"]
                if "https:" not in url:
                    url = "https:" + url

                html = get_html(url)
                tags = get_tags_from_url(html)
                title = get_title_from_url(html)
                time_bought = get_datetime_from_unix(item["utc_date"])

                entry = {
                    "amount_paid_usd": item["amount_paid_usd"],
                    "tags": tags,
                    "country": item["country"],
                    "title": title,
                    "artist": item["artist_name"],
                    "at": time_bought,
                    "type": item_type,
                    "image": item["art_url"]
                }
                data.append(entry)

    return data
