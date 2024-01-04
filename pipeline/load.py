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

            # if Levenshtein.normalized_similarity(
            # genre[1], new_genre.lower()) > 0.75:
            #     return

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

def load_countries(new_country, db_connection: extensions.connection):
    """
    Finds any unique countries and uploads them to the table.
    """
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM country;")

        countries = cur.fetchall()
        for country in countries:
            if country[1] == new_country:
                return
        
        cur.execute(f"INSERT INTO country(country) VALUES ('{new_country}')")
        db_connection.commit()

def load_items(new_item, db_connection: extensions.connection):
    """
    Finds any unique items and finds all the relevant data to upload to the table.
    """
    with db_connection.cursor() as cur:
        cur.execute(f"SELECT * FROM item;")
        items = cur.fetchall()

        for item in items:
            if item[2] == new_item['title']:
                return
                
            
        cur.execute(f"SELECT item_type_id FROM item_type WHERE item_type='{new_item['type']}'")
        item_type_id = cur.fetchone()[0]
        cur.execute(f"SELECT artist_id FROM artist WHERE artist_name='{new_item['artist'].lower()}'")
        artist_id = cur.fetchone()[0]
        new_item['title'] = new_item['title'].replace("'", "''")
        cur.execute(f"""INSERT INTO item(item_type_id, item_name, artist_id, item_image) 
                    VALUES ({item_type_id}, '{new_item['title']}', {artist_id}, '{new_item['image']}')""")
        db_connection.commit()
        
def load_item_genres(new_item, db_connection):
    """
    Connects to items to the genres. Currently will store duplicates.
    """
    with db_connection.cursor() as cur:
        new_item['title'] = new_item['title'].replace("'", "''")
        cur.execute(f"SELECT item_id FROM item WHERE item_name = '{new_item['title']}'")
        item_id = cur.fetchone()[0]
        
        cur.execute(f"SELECT genre_id FROM genre WHERE genre='{new_item['tags'].lower()}'")
        genre_id = cur.fetchone()[0]
        cur.execute(f"INSERT INTO item_genre(item_id, genre_id) VALUES ({item_id},{genre_id})")
        db_connection.commit()

def load_sales_event(new_sale, db_connection):
    pass

if __name__ == "__main__":
    load_dotenv()
    music_df = pd.read_csv("clean_data.csv")

    con = get_db_connection()
    #music_df['tags'].apply(load_genres, db_connection=con)
    #music_df['artist'].apply(load_artists, db_connection=con)
    #music_df['country'].apply(load_countries, db_connection=con)
    #music_df.apply(load_items, db_connection=con, axis=1)
    #music_df.apply(load_item_genres, db_connection=con, axis=1)
