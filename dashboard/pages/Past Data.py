"""Past data page for the StreamLit dashboard, where users can download specific past data."""
from os import environ
import pandas as pd
import streamlit as st
from psycopg2 import connect, extensions


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
                    SELECT sale_event.*, country.country, item.item_name, item.item_type_id, item.item_image, artist.artist_name, genre.genre
                    FROM sale_event
                    JOIN country
                    ON country.country_id = sale_event.country_id
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN artist
                    ON artist.artist_id = item.artist_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id = item_genre.genre_id;""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'item_id',
                        'country_id', 'country', 'item_name', 'item_type', 'item_image', 'artist', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


if __name__ == "__main__":
    st.set_page_config(
        layout="wide", page_title="BandCamp Analysis", page_icon="🎵")

    st.write("# Past Data")

    with st.container(border=True):

        st.write("Not sure if we should keep this page!")

        st.write("Make selections below to download the right data for you.")

        conn = get_db_connection()
        data = load_all_data(conn)

        csv_file_path = 'test.csv'

        st.markdown(f'CSV file [here]({csv_file_path})')
