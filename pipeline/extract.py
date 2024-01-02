"""Script to interact with the Bandcamp API and extract relevant information"""
from datetime import datetime

import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup

EPOCH = datetime.utcfromtimestamp(0)


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
        f"https://bandcamp.com/api/salesfeed/1/get?start_date={seconds}")

    return response.json()


def get_genre_from_url(url: str):
    page = urlopen(url)
    html_bytes = page.read()
    html_doc = html_bytes.decode("utf_8")

    soup = BeautifulSoup(html_doc, "html.parser")
    # print(soup)

    for tag in soup.find_all('a', {'class': 'tag'}):
        print(tag.text)


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

                entry = {
                    "amount_paid_usd": item["amount_paid_usd"],
                    "url": url
                }
                data.append(entry)

    return data


if __name__ == "__main__":
    # print(unix_time_millis(datetime.now()))
    sales_data = load_sales_data(datetime.now())
    data = extract_data_from_json(sales_data)
    url = "https://ericksermon.bandcamp.com/track/wit-ees"
    get_genre_from_url(url)
