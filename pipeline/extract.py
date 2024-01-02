"""Script to interact with the Bandcamp API and extract relevant information"""
from datetime import datetime
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup

EPOCH = datetime.utcfromtimestamp(0)
TIMEOUT = 20


def unix_time_millis(dt: datetime) -> float:
    """
    Given a datetime, return the time in seconds since epoch.
    """
    return (dt - EPOCH).total_seconds() * 1000.0


def load_sales_data(dt: datetime) -> dict:
    """
    Uses the bandcamp API to return all the sales data from the last minute in json format.
    """
    seconds = unix_time_millis(dt) - 60
    response = requests.get(
        f"https://bandcamp.com/api/salesfeed/1/get?start_date={seconds}", timeout=TIMEOUT)

    return response.json()


def get_tags_from_url(url: str) -> list[str]:
    """
    Given a url for a track, return the associated tags for the track
    """
    tags = []

    with urlopen(url) as page:
        html_bytes = page.read()
        html_doc = html_bytes.decode("utf_8")

    soup = BeautifulSoup(html_doc, "lxml")

    for tag in soup.find_all("a", {"class": "tag"}):
        tags.append(tag.text)

    return tags


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
                url = item["url"]
                if "https:" not in url:
                    url = "https:" + url

                tags = get_tags_from_url(url)

                entry = {
                    "amount_paid_usd": item["amount_paid_usd"],
                    "url": url,
                    "tags": tags
                }
                data.append(entry)

    return data


if __name__ == "__main__":
    sales_data = load_sales_data(datetime.now())
    extracted_data = extract_data_from_json(sales_data)
    print(extracted_data)
