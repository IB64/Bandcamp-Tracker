"""Script which loads data to the tables in the database"""
from os import environ

from psycopg2 import extensions, connect
import pandas as pd
from rapidfuzz.distance import Levenshtein
from dotenv import load_dotenv


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


def load_genres(new_genre: str, db_connection: extensions.connection) -> str:
    """
    Finds any unique genres or genres with a less then 75% match to any in the table 
    and uploads them to the table
    """

    with db_connection.cursor() as cur:
        #     cur.execute("SELECT * FROM genre ORDER BY genre_id;")

        #     genres = cur.fetchall()
        #     genres = {r[1]: r[0] for r in genres}
        #     try:
        #         if genres[new_genre.lower()]:
        #             return new_genre.lower()
        #     except KeyError:

        #     if Levenshtein.normalized_similarity(
        #             genre[1], new_genre.lower()) > 0.75:
        #         return genre[1]

        new_genre = new_genre.replace("'", "''")
        cur.execute(
            f"INSERT INTO genre(genre) VALUES ('{new_genre.lower()}') ON CONFLICT DO NOTHING;")
        db_connection.commit()
        return new_genre


def load_artists(new_artist: str, db_connection: extensions.connection) -> None:
    """
    Finds any unique artists and uploads them to the table.
    """

    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM artist;")

        artists = cur.fetchall()
        artists = {r[1]: r[0] for r in artists}
        try:
            if artists[new_artist.lower()]:
                return
        except KeyError:

            new_artist = new_artist.replace("'", "''")
            cur.execute(
                f"INSERT INTO artist(artist_name) VALUES ('{new_artist.lower()}');")
            db_connection.commit()


def load_countries(new_country: str, db_connection: extensions.connection) -> None:
    """
    Finds any unique countries and uploads them to the table.
    """
    with db_connection.cursor() as cur:
        # cur.execute("SELECT * FROM country;")

        # countries = cur.fetchall()
        # countries = {r[1]: r[0] for r in countries}
        # try:
        #     if countries[new_country.title()]:
        #         return
        # except KeyError:

        cur.execute(
            f"INSERT INTO country(country) VALUES ('{new_country}') ON CONFLICT DO NOTHING")
        db_connection.commit()


def load_items(new_item: pd.Series, db_connection: extensions.connection) -> None:
    """
    Finds any unique items and finds all the relevant data to upload to the table.
    """
    with db_connection.cursor() as cur:
        cur.execute(f"SELECT * FROM item;")
        items = cur.fetchall()
        items = {r[2]: r[0] for r in items}

        try:
            if items[new_item['title']]:
                return
        #  if a new title is found, insert relevant details to db
        except KeyError:

            cur.execute(
                f"SELECT item_type_id FROM item_type WHERE item_type='{new_item['type']}'")
            item_type_id = cur.fetchone()[0]
            new_item['artist'] = new_item['artist'].replace("'", "''")
            cur.execute(f"""SELECT artist_id FROM artist
                        WHERE artist_name='{new_item['artist'].lower()}'""")
            artist_id = cur.fetchone()[0]
            new_item['title'] = new_item['title'].replace("'", "''")
            cur.execute(f"""INSERT INTO item(item_type_id, item_name, artist_id, item_image)
                        VALUES ({item_type_id}, '{new_item['title']}',
                        {artist_id}, '{new_item['image']}')""")
            db_connection.commit()


def load_item_genres(new_item: pd.Series, db_connection: extensions.connection) -> None:
    """
    Connects the items to the genres through table IDs.
    """
    with db_connection.cursor() as cur:
        new_item['title'] = new_item['title'].replace("'", "''")
        cur.execute(
            f"SELECT item_id FROM item WHERE item_name = '{new_item['title']}'")
        item_id = cur.fetchone()[0]

        new_item['tags'] = new_item['tags'].replace("'", "''")
        cur.execute(
            f"SELECT genre_id FROM genre WHERE genre = '{new_item['tags'].lower()}'")
        genre_id = cur.fetchone()[0]

        cur.execute(f"SELECT * FROM item_genre;")
        # item_genres = cur.fetchall()
        # for item_genre in item_genres:
        #     if item_genre[1] == item_id and item_genre[2] == genre_id:
        #         return

        item_genres = cur.fetchall()
        item_genres = {r[2]: r[1] for r in item_genres}

        try:
            if item_genres[genre_id] == item_id:
                return
        except KeyError:
            pass

        cur.execute(
            f"INSERT INTO item_genre(item_id, genre_id) VALUES ({item_id},{genre_id})")
        print("Added")
        db_connection.commit()


def load_sales_event(new_sale: pd.Series, db_connection: extensions.connection) -> None:
    """
    Uploads all the sales events to the sales event table.
    """
    with db_connection.cursor() as cur:
        cur.execute(
            f"SELECT country_id FROM country WHERE country='{new_sale['country']}';")
        country_id = cur.fetchone()[0]

        new_sale['title'] = new_sale['title'].replace("'", "''")
        cur.execute(
            f"SELECT item_id FROM item WHERE item_name='{new_sale['title']}'")
        item_id = cur.fetchone()[0]

        cur.execute(f"""INSERT INTO sale_event(sale_time, amount, country_id, item_id)
                    VALUES ('{new_sale['at']}', {new_sale['amount_paid_usd']}, 
                    {country_id}, {item_id}) """)
        db_connection.commit()


def load_data(sale_df_flat_tags: pd.DataFrame, sale_df: pd.DataFrame, con: extensions.connection) -> None:
    """
    Finds all the required data to be uploaded to the correct table.
    """
    sale_df_flat_tags['tags'] = sale_df_flat_tags['tags'].apply(
        load_genres, db_connection=con)
    print("Tags added")
    sale_df_flat_tags['artist'].apply(load_artists, db_connection=con)
    print("Artists added")
    sale_df_flat_tags['country'].apply(load_countries, db_connection=con)
    print("Countries added")
    sale_df_flat_tags.apply(load_items, db_connection=con, axis=1)
    print("Items added")
    sale_df_flat_tags.apply(load_item_genres, db_connection=con, axis=1)
    print("Item genres added")
    sale_df.apply(load_sales_event, db_connection=con, axis=1)
    print("Sales added")


if __name__ == "__main__":
    load_dotenv()

    con = get_db_connection()
    load_item_genres("Nicolas Amaro & Wilowm - Tecnonarin", con)
