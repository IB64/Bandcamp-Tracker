from os import environ

from psycopg2 import extensions, connect
from dotenv import load_dotenv
import pandas as pd

GENRES_NOT_IN_DB = []
GENRES_IN_DB = []

ARTISTS_NOT_IN_DB = []
ARTISTS_IN_DB = []

COUNTRIES_NOT_IN_DB = []
COUNTRIES_IN_DB = []

ITEMS_NOT_IN_DB = set()
ITEMS_IN_DB = []

TAGS_NOT_IN_DB = []


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


def get_countries(db_connection: extensions.connection) -> dict:
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM country;")

        countries = cur.fetchall()
        return {r[1]: r[0] for r in countries}


def get_items(db_connection: extensions.connection) -> dict:
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM item;")

        items = cur.fetchall()
        return {r[2]: r[0] for r in items}


def check_if_genre_in_db(new_genre: str, genres: dict):
    if new_genre.lower() not in genres.keys():
        new_genre = new_genre.replace("'", "`")
        GENRES_NOT_IN_DB.append((new_genre.lower(),))
    else:
        GENRES_IN_DB.append(new_genre.lower())


def check_if_artist_in_db(new_artist: str, artists: dict):
    if new_artist.lower() not in artists.keys():
        new_artist = new_artist.replace("'", "`")
        ARTISTS_NOT_IN_DB.append((new_artist.lower(),))
    else:
        ARTISTS_IN_DB.append(new_artist.lower())


def check_if_country_in_db(new_country: str, countries: dict):
    if new_country not in countries.keys():
        new_country = new_country.replace("'", "`")
        COUNTRIES_NOT_IN_DB.append((new_country,))
    else:
        COUNTRIES_IN_DB.append(new_country)


def check_if_item_in_db(new_item: str, items: dict, db_connection: extensions.connection):
    new_item['title'] = new_item['title'].replace("'", "`")
    new_item['artist'] = new_item['artist'].replace("'", "`")
    if new_item['title'] not in items.keys():
        with db_connection.cursor() as cur:
            cur.execute(
                f"SELECT item_type_id FROM item_type WHERE item_type='{new_item['type']}'")
            item_type_id = cur.fetchone()[0]
            cur.execute(f"""SELECT artist_id FROM artist
                        WHERE artist_name='{new_item['artist'].lower()}'""")
            artist_id = cur.fetchone()[0]
            ITEMS_NOT_IN_DB.add(
                (new_item['title'], artist_id, item_type_id, new_item['image']))
            TAGS_NOT_IN_DB.append(new_item['tags'])
    else:
        ITEMS_IN_DB.append(new_item['title'])


def add_genres_to_database(db_connection: extensions.connection, list: list[str]):
    with db_connection.cursor() as cur:
        query = f"""
            INSERT INTO genre(genre) VALUES (%s) ON CONFLICT DO NOTHING;
            """

        cur.executemany(query, list)
        db_connection.commit()
        print(f"Genres added!")


def add_artists_to_database(db_connection: extensions.connection, list: list[str]):
    with db_connection.cursor() as cur:
        query = f"""
            INSERT INTO artist(artist_name) VALUES (%s) ON CONFLICT DO NOTHING;
            """

        cur.executemany(query, list)
        db_connection.commit()
        print("Artists added!")


def add_countries_to_database(db_connection: extensions.connection, list: list[str]):
    with db_connection.cursor() as cur:
        query = f"""
            INSERT INTO country(country) VALUES (%s) ON CONFLICT DO NOTHING;
            """

        cur.executemany(query, list)
        db_connection.commit()
        print("Countries added!")


def add_items_to_database(db_connection: extensions.connection, list: list[tuple]):
    with db_connection.cursor() as cur:
        query = f"""
            INSERT INTO item(item_name, artist_id, item_type_id, item_image)
            VALUES (%s, %s, %s, %s);
        """

        cur.executemany(query, list)
        db_connection.commit()
        print("Items added!")


def add_item_genres_to_database(db_connection: extensions.connection, list: list[tuple], tags: list[tuple]):
    count = 0
    item_genres = []
    with db_connection.cursor() as cur:
        for item in list:
            cur.execute(
                f"SELECT item_id FROM item WHERE item_name = '{item[0]}'")
            item_id = cur.fetchone()[0]

            tags[count] = tags[count].replace("'", "`")
            cur.execute(
                f"SELECT genre_id FROM genre WHERE genre = '{tags[count].lower()}'")
            genre_id = cur.fetchone()[0]

            item_genres.append((item_id, genre_id))

            count += 1

        query = f"""
            INSERT INTO item_genre (item_id, genre_id) VALUES (%s, %s);
            """

        cur.executemany(query, item_genres)
        db_connection.commit()
        print("Added Item Genres!")


def add_sales_events(new_sale: pd.Series, db_connection: extensions.connection):
    with db_connection.cursor() as cur:
        cur.execute(
            f"SELECT country_id FROM country WHERE country='{new_sale['country']}';")
        country_id = cur.fetchone()[0]

        new_sale['title'] = new_sale['title'].replace("'", "`")
        cur.execute(
            f"SELECT item_id FROM item WHERE item_name='{new_sale['title']}'")
        item_id = cur.fetchone()[0]

        cur.execute(f"""INSERT INTO sale_event(sale_time, amount, country_id, item_id)
                    VALUES ('{new_sale['at']}', {new_sale['amount_paid_usd']}, 
                    {country_id}, {item_id}) """)
        db_connection.commit()


def load(db_connection: extensions.connection, flat_dataframe: pd.DataFrame, not_flat_dataframe: pd.DataFrame):
    db_genres = get_genres(db_connection)
    db_artists = get_artists(db_connection)
    db_countries = get_countries(db_connection)
    db_items = get_items(db_connection)

    flat_dataframe['tags'].apply(check_if_genre_in_db, genres=db_genres)
    add_genres_to_database(db_connection, GENRES_NOT_IN_DB)

    flat_dataframe['artist'].apply(check_if_artist_in_db, artists=db_artists)
    add_artists_to_database(db_connection, ARTISTS_NOT_IN_DB)

    flat_dataframe['country'].apply(
        check_if_country_in_db, countries=db_countries)
    add_countries_to_database(db_connection, COUNTRIES_NOT_IN_DB)

    flat_dataframe.apply(check_if_item_in_db, items=db_items,
                         db_connection=db_connection, axis=1)
    add_items_to_database(db_connection, ITEMS_NOT_IN_DB)

    add_item_genres_to_database(db_connection, ITEMS_NOT_IN_DB, TAGS_NOT_IN_DB)

    not_flat_dataframe.apply(
        add_sales_events, db_connection=db_connection, axis=1)
    print("Sales Added!")


if __name__ == "__main__":
    load_dotenv()

    not_in_db = []
    in_db = []

    print(not_in_db)
    print(in_db)
