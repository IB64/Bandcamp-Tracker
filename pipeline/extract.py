"""
Script to interact with the Bandcamp API and extract relevant information
"""
from datetime import datetime
from time import perf_counter
from urllib.request import urlopen

from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.exceptions import Timeout, HTTPError

EPOCH = datetime.utcfromtimestamp(0)
TIMEOUT = 20
ALBUM = "a"
TRACK = "t"
FIVE_MINS_IN_SECONDS = 300


def unix_time_millis(dt: datetime) -> float:
    """
    Given a datetime, return the time in seconds since epoch.
    """
    return (dt - EPOCH).total_seconds() * 1000.0


def load_sales_data(dt: datetime) -> dict:
    """
    Uses the bandcamp API to return all the sales data from the last 5 minute in json format.
    """
    seconds = unix_time_millis(dt) - FIVE_MINS_IN_SECONDS
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
    return a list of dictionaries with each dictionary containing
    wanted information for each sale.
    """
    data = []

    events = sales_json["events"]
    for event in events:
        if event["event_type"] == "sale":
            items = event["items"]
            for item in items:
                if item["item_type"] not in (ALBUM, TRACK):
                    continue

                url = item["url"]
                if "https:" not in url:
                    url = "https:" + url

                html = get_html(url)
                tags = get_tags_from_url(html)
                title = get_title_from_url(html)

                entry = {
                    "amount_paid_usd": item["amount_paid_usd"],
                    "tags": tags,
                    "country": item["country"],
                    "title": title,
                    "artist": item["artist_name"],
                    "at": datetime.utcfromtimestamp(item["utc_date"]).strftime("%m/%d/%Y, %H:%M:%S")
                }
                data.append(entry)

    df = pd.DataFrame(data)
    df.to_csv("data.csv", index=False)
    return df


if __name__ == "__main__":
    start = perf_counter()
    sales_data = load_sales_data(datetime.now())
    extracted_data = extract_data_from_json(sales_data)

    print(f"Time taken: {perf_counter() - start}")
