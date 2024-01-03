"""Script that creates a pdf daily report for the previous days sales data"""

from os import environ
from datetime import datetime, timedelta

from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd


def get_db_connection() -> extensions.connection:
    """Returns a connection to the AWS Bandcamp database"""

    try:
        return connect(user=environ["DB_USER"],
                       password=environ["DB_PASSWORD"],
                       host=environ["DB_IP"],
                       port=environ["DB_PORT"],
                       database=environ["DB_NAME"])
    except ConnectionError:
        print("Error: Cannot connect to the database")


def load_artist_data(db_connection) -> pd.DataFrame:
    """Loads all the artist data"""

    with db_connection.cursor() as curr:

        curr.execute("""
        SELECT * FROM artist;""")

        tupples = curr.fetchall()

        column_names = ['artist_id', 'artist_name']

        df = pd.DataFrame(tupples, columns=column_names)

        return df


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    artists = load_artist_data(connection)

    print(artists)
