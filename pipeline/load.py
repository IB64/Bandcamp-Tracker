"""Script which loads data to the tables in the database"""
from os import environ

from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd
from rapidfuzz.distance import Levenshtein


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

def load_genres(new_genre, db_connection: extensions.connection):
    """
    Finds any unique genres or genres with a less then 75% match to any in the table 
    and uploads them to the table
    """

    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM genre;")


        genres = cur.fetchall()
        for genre in genres:
            if genre[1] == new_genre.lower():
                return

            if Levenshtein.normalized_similarity(
            genre[1], new_genre.lower()) > 0.75:
                return

        cur.execute(f"INSERT INTO genre(genre) VALUES ('{new_genre.lower()}');")
        db_connection.commit()

def load_artists(new_artist, db_connection: extensions.connection):
    """
    Finds any unique artists and uploads them to the table.
    """

    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM artist;")


        artists = cur.fetchall()
        for artist in artists:
            if artist[1] == new_artist.lower():
                return

        cur.execute(f"INSERT INTO artist(artist_name) VALUES ('{new_artist.lower()}');")
        db_connection.commit()


if __name__ == "__main__":
    load_dotenv()
    music_df = pd.read_csv("clean_data.csv")

    con = get_db_connection()
    music_df['tags'].apply(load_genres, db_connection=con)
    music_df['artist'].apply(load_artists, db_connection=con)
