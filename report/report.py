"""Script that creates a pdf daily report for the previous days sales data"""

from os import environ
from datetime import datetime, timedelta

from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd
import pdfkit

YESTERDAY_DATE = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y')


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


def load_all_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.*, country.country, album.album_name, artist.artist_name, genre.genre
                    FROM sale_event
                    JOIN country
                    ON country.country_id = sale_event.country_id
                    JOIN album
                    ON album.album_id = sale_event.album_id
                    JOIN artist
                    ON artist.artist_id = album.artist_id
                    JOIN album_genre
                    ON album_genre.album_id = album.album_id
                    JOIN genre
                    ON genre.genre_id = album_genre.genre_id;""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'album_id',
                        'country_id', 'country', 'album_name', 'artist', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_yesterday_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.*, country.country, album.album_name, artist.artist_name, genre.genre
                    FROM sale_event
                    JOIN country
                    ON country.country_id = sale_event.country_id
                    JOIN album
                    ON album.album_id = sale_event.album_id
                    JOIN artist
                    ON artist.artist_id = album.artist_id
                    JOIN album_genre
                    ON album_genre.album_id = album.album_id
                    JOIN genre
                    ON genre.genre_id = album_genre.genre_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'album_id',
                        'country_id', 'country', 'album_name', 'artist', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_key_analytics(data: pd.DataFrame) -> tuple:
    """Returns the total sales and the total income for the day"""

    # total number of sales
    total_transactions = data['sale_id'].nunique()

    # total income
    total_income = (data['amount'].sum())/100

    return total_transactions, total_income


def get_top_3_popular_artists(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 most popular artists for the previous day"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    popular_artists = unique_sales['artist'].value_counts().head(
        3).reset_index()

    return popular_artists


def get_top_3_grossing_artists(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 selling artists for the previous day"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    artist_sales = unique_sales.groupby(
        'artist')['amount'].sum()
    artist_sales = (artist_sales/100).head(3).reset_index()

    return artist_sales


def get_top_3_sold_albums(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 3 sold albums which includes the album name, artist, genre and amount"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    popular_albums = unique_sales['album_name'].value_counts().head(
        3).reset_index()

    return popular_albums


def get_popular_genre(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 3 genres"""

    unique_genre_count = data['genre'].value_counts().head(3).reset_index()

    return unique_genre_count


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)

    yesterdays_data = load_yesterday_data(connection)

    key_metrics = get_key_analytics(all_data)

    top_3_popular_artists = get_top_3_popular_artists(all_data)
    top_3_grossing_artists = get_top_3_grossing_artists(all_data)

    top_genres = get_popular_genre(all_data)
    top_albums = get_top_3_sold_albums(all_data)

    print(all_data)
    print('-------')
    print(top_3_popular_artists)
    print('-------')
    print(top_3_grossing_artists)
    print('-------')
    print(top_genres)
    print('-------')
    print(top_albums)
    print('-------')

    print(
        f"The total number of sales on {YESTERDAY_DATE} were {key_metrics[0]}")
    print(f"The total income was ${key_metrics[1]}")
