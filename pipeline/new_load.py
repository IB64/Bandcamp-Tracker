from os import environ

from psycopg2 import extensions, connect
from dotenv import load_dotenv
import pandas as pd

GENRES_NOT_IN_DB = []
GENRES_IN_DB = []

ARTISTS_NOT_IN_DB = []
ARTISTS_IN_DB = []


def get_db_connection() -> extensions.connection:
    """
    Returns a connection to the AWS Bandcamp database
    """

    try:
        return connect(user=environ["DB_USER"],
                       password=environ["DB_PASSWORD"],
                       host=environ["DB_IP"],
                       port=environ["DB_PORT"],
                       database=environ["DB_NAME"])
    except ConnectionError:
        print("Error: Cannot connect to the database")


def get_genres(db_connection: extensions.connection) -> dict:
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM genre;")
        genres = cur.fetchall()
        return {r[1]: r[0] for r in genres}


def get_artists(db_connection: extensions.connection) -> dict:
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM artist;")

        artists = cur.fetchall()
        return {r[1]: r[0] for r in artists}


def check_if_genre_in_db(new_genre: str, genres: dict):
    if new_genre.lower() not in genres.keys():
        GENRES_NOT_IN_DB.append((new_genre,))
    else:
        GENRES_IN_DB.append(new_genre)


def check_if_artists_in_db(new_artist: str, artists: dict):
    if new_artist.lower() not in artists.keys():
        ARTISTS_NOT_IN_DB.append((new_artist,))
    else:
        ARTISTS_IN_DB.append(new_artist)


def add_genres_to_database(db_connection: extensions.connection, list: list[str]):
    with db_connection.cursor() as cur:
        query = f"""
            INSERT INTO genre(genre) VALUES (%s);
            """

        cur.executemany(query, list)
        print(f"Genres added!")


def add_artists_to_database(db_connection: extensions.connection, list: list[str]):
    with db_connection.cursor() as cur:
        query = f"""
            INSERT INTO artist(artist_name) VALUES (%s);
            """
        print(list)


def load(db_connection: extensions.connection, dataframe: pd.DataFrame):
    db_genres = get_genres(db_connection)
    db_artists = get_artists(db_connection)

    dataframe['tags'].apply(check_if_genre_in_db, genres=db_genres)
    dataframe['artist'].apply(check_if_artists_in_db, artists=db_artists)

    add_genres_to_database(db_connection, GENRES_NOT_IN_DB)


if __name__ == "__main__":
    load_dotenv()

    not_in_db = []
    in_db = []

    print(not_in_db)
    print(in_db)
