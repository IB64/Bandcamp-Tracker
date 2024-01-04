"""Script that creates a pdf daily report for the previous days sales data"""

from os import environ
from datetime import datetime, timedelta

from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd
from fpdf import FPDF


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
                    SELECT sale_event.*, country.country,item_name, artist.artist_name, genre.genre, item_type.item_type
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
                    ON genre.genre_id =item_genre.genre_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id;""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'item_id',
                        'country_id', 'country', 'item_name', 'artist', 'genre', 'item_type']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_yesterday_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.*, country.country, item_name, artist.artist_name, genre.genre, item_type.item_type
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
                    ON genre.genre_id = item_genre.genre_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'item_id',
                        'country_id', 'country', 'item_name', 'artist', 'genre', 'item_type']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_key_analytics(data: pd.DataFrame) -> tuple:
    """Returns the total sales and the total income for the day"""

    # total number of sales
    total_sales = data['sale_id'].nunique()

    # total income
    total_income = (data['amount'].sum())/100

    return total_sales, total_income


def get_top_5_popular_artists(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 most popular artists for the previous day"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    popular_artists = unique_sales['artist'].value_counts().head(
        5).reset_index()

    return popular_artists


def get_top_5_grossing_artists(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 selling artists for the previous day"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    artist_sales = unique_sales.groupby(
        'artist')['amount'].sum()
    artist_sales = (artist_sales/100).head(5).reset_index()

    return artist_sales


def remove_duplicate_words(words: list) -> list:
    """Removes duplicate words in a list"""

    return list(set(words))


def get_top_5_sold_albums(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 sold albums which includes the item name, artist, genre and amount"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    popular_albums = album_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    return popular_albums


def get_top_5_sold_tracks(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 sold items which includes theitem name, artist, genre and amount"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    popular_tracks = track_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    return popular_tracks


def get_top_5_grossing_albums(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 albums that are earning the most money"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    album_sales = album_sales.groupby(
        'item_name')['amount'].sum()
    album_sales = (
        album_sales/100).sort_values(ascending=False).head(5).reset_index()

    return album_sales


def get_top_5_grossing_tracks(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 tracks that are earning the most money"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    track_sales = track_sales.groupby(
        'item_name')['amount'].sum()
    track_sales = (
        track_sales/100).sort_values(ascending=False).head(5).reset_index()

    return track_sales


def get_album_genres(data: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    """Returns a dataframe of album and its genre if the album is in a specified list of albums"""

    album_sales = data[data['item_type'] == 'album']

    filtered_album_sales = album_sales[album_sales['item_name'].isin(selected)]

    albums = filtered_album_sales.groupby(['item_name', 'artist'])[
        'genre'].agg(list).reset_index()

    albums['genre'] = albums['genre'].apply(
        remove_duplicate_words)

    return albums


def get_track_genres(data: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    """Returns a dataframe of the top 5 sold albums which includes the album name, artist, genre and amount"""

    track_sales = data[data['item_type'] == 'track']

    filtered_track_sales = track_sales[track_sales['item_name'].isin(selected)]

    tracks = filtered_track_sales.groupby(['item_name', 'artist'])[
        'genre'].agg(list).reset_index()

    tracks['genre'] = tracks['genre'].apply(
        remove_duplicate_words)

    return tracks


def get_popular_genre(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 genres"""

    unique_genre_count = data['genre'].value_counts().head(5).reset_index()

    return unique_genre_count


def get_countries_with_most_sales(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe that shows how many sales are happening in each country."""

    country_sales = data['country'].value_counts(
    ).sort_values(ascending=False).reset_index()

    return country_sales


def get_popular_artist_per_country(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe that shows which artist is the most popular in each country"""

    most_popular_artists = data.groupby('country')['artist'].apply(
        lambda x: x.value_counts().idxmax()).reset_index()

    return most_popular_artists


def get_number_of_sold_albums_and_tracks(data: pd.DataFrame) -> tuple:
    """Returns a dataframe that shows the number of albums and tracks sold"""

    albums_sold = data[data['item_type'] == 'album'].value_counts()
    tracks_sold = data[data['item_type'] == 'tracks'].value_counts()

    return albums_sold, tracks_sold


def generate_pdf_report(data):
    """Generates a pdf report that contains all the analysis for the previous day"""

    pdf = FPDF('P')
    pdf.add_page()
    pdf.set_font("Arial", "B", size=24)

    pdf.cell(
        200, 10, txt=f"BandCamp Daily Report - {YESTERDAY_DATE}", ln=True, align='C')
    pdf.ln(10)

    # Key Metrics
    pdf.set_font("Arial", "B", size=18)
    key_metrics = get_key_analytics(data)
    albums_sold, tracks_sold = get_number_of_sold_albums_and_tracks(data)
    pdf.cell(200, 10, txt='Overview', ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 5, txt=f"Total number of sales: {key_metrics[0]}", ln=True)
    pdf.cell(200, 5, txt=f"Total Income: ${key_metrics[1]}", ln=True)
    pdf.cell(
        200, 5, txt=f"Total number of albums sold: ${albums_sold}", ln=True)
    pdf.cell(
        200, 5, txt=f"Total number of tracks sold: ${tracks_sold}", ln=True)
    pdf.ln(10)

    # Artists Data
    top_5_popular_artists = get_top_5_popular_artists(data)
    pdf.set_font("Arial", "B", size=18)
    pdf.multi_cell(100, 10, txt=f"----Top 5 Popular Artists----")
    pdf.set_font("Arial", size=12)
    pdf.multi_cell(
        100, 10, txt=f"Top 5 artists that have sold the most items")
    table_data = top_5_popular_artists.to_string(index=False).split('\n')
    for row in table_data:
        pdf.multi_cell(100, 10, txt=row)
    pdf.ln(8)

    top_5_grossing_artists = get_top_5_grossing_artists(data)
    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top 5 Grossing Artists-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top 5 artists that are earning the most", ln=True)
    table_data = top_5_grossing_artists.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    # Album and Track Data
    top_5_albums = get_top_5_sold_albums(data)
    top_5_tracks = get_top_5_sold_tracks(data)

    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top 5 Popular Albums-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top 5 albums sold", ln=True)
    table_data = top_5_albums.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top 5 Popular Tracks-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top 5 tracks sold", ln=True)
    table_data = top_5_tracks.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    top_5_grossing_albums = get_top_5_grossing_albums(data)
    top_5_grossing_tracks = get_top_5_grossing_tracks(data)

    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top 5 Grossing Albums-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top 5 albums earning the most", ln=True)
    table_data = top_5_grossing_albums.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top 5 Grossing Tracks-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top 5 tracks earning the most", ln=True)
    table_data = top_5_grossing_tracks.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    top_5_albums_list = top_5_albums['item_name'].tolist()
    top_5_tracks_list = top_5_tracks['item_name'].tolist()

    album_genres = get_album_genres(all_data, top_5_albums_list)
    track_genres = get_track_genres(all_data, top_5_tracks_list)

    # Genre Data
    top_genres = get_popular_genre(all_data)
    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top 5 Genres-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top 5 genres that were most bought", ln=True)
    table_data = top_genres.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    # Country Sales
    sales_per_country = get_countries_with_most_sales(all_data)
    artists_per_country = get_popular_artist_per_country(all_data)

    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Sales Per Country-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the number os sales there have been per country", ln=True)
    table_data = sales_per_country.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    pdf.set_font("Arial", "B", size=18)
    pdf.cell(200, 10, txt=f"-------Top Artist per country-------", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(
        200, 10, txt=f"This is showing the top Artist for every country", ln=True)
    table_data = artists_per_country.to_string(index=False).split('\n')
    for row in table_data:
        pdf.cell(200, 10, txt=row, ln=True)
    pdf.ln(8)

    return pdf.output(f"Bandcamp-Report.pdf")


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)

    yesterdays_data = load_yesterday_data(connection)

    print(all_data)

    print(generate_pdf_report(all_data))
