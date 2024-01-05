"""Script that creates a pdf daily report for the previous days sales data"""

from os import environ
from datetime import datetime, timedelta

from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd
from xhtml2pdf import pisa

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
                    SELECT sale_event.*, country.country, artist.artist_name, genre.genre, item_type.item_type, item.item_name
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
                        'country_id', 'country', 'artist', 'genre', 'item_type', 'item_name']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_yesterday_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.*, country.country, artist, artist.artist_name, genre.genre, item_type.item_type
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
                        'country_id', 'country', 'artist', 'artist', 'genre', 'item_type']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_key_analytics(data: pd.DataFrame) -> tuple:
    """Returns the amount sales and the amount income for the day"""

    # amount number of sales
    amount_sales = data['sale_id'].nunique()

    # amount income
    amount_income = (data['amount'].sum())/100

    return amount_sales, amount_income


def get_top_5_popular_artists(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 most popular artists for the previous day"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    popular_artists = unique_sales['artist'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    return popular_artists.to_dict('records')


def get_top_5_grossing_artists(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 selling artists for the previous day"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    artist_sales = unique_sales.groupby(
        'artist')['amount'].sum()
    artist_sales = (
        artist_sales/100).sort_values(ascending=False).head(5).reset_index()

    return artist_sales.to_dict('records')


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

    return popular_albums.to_dict('records')


def get_top_5_sold_tracks(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 sold items which includes the item name, artist, genre and amount"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    popular_tracks = track_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    return popular_tracks.to_dict('records')


def get_top_5_grossing_albums(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 albums that are earning the most money"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    album_sales = album_sales.groupby(
        'item_name')['amount'].sum()
    album_sales = (
        album_sales/100).sort_values(ascending=False).head(5).reset_index()

    return album_sales.to_dict('records')


def get_top_5_grossing_tracks(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 tracks that are earning the most money"""

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    track_sales = track_sales.groupby(
        'item_name')['amount'].sum()
    track_sales = (
        track_sales/100).sort_values(ascending=False).head(5).reset_index()

    return track_sales.to_dict('records')


def get_album_genres(data: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    """Returns a dataframe of album and its genre if the album is in a specified list of albums"""

    album_sales = data[data['item_type'] == 'album']

    filtered_album_sales = album_sales[album_sales['artist'].isin(selected)]

    albums = filtered_album_sales.groupby(['artist', 'artist'])[
        'genre'].agg(list).reset_index()

    albums['genre'] = albums['genre'].apply(
        remove_duplicate_words)

    return albums


def get_track_genres(data: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    """Returns a dataframe of the top 5 sold albums which includes the album name, artist, genre and amount"""

    track_sales = data[data['item_type'] == 'track']

    filtered_track_sales = track_sales[track_sales['artist'].isin(selected)]

    tracks = filtered_track_sales.groupby(['artist', 'artist'])[
        'genre'].agg(list).reset_index()

    tracks['genre'] = tracks['genre'].apply(
        remove_duplicate_words)

    return tracks


def get_popular_genre(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the top 5 genres"""

    unique_genre_count = data['genre'].value_counts().head(5).reset_index()

    return unique_genre_count.to_dict('records')


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


def generate_html_string(data: pd.DataFrame) -> str:
    """Returns a html string that contains all the daily report anlayses"""

    key_metrics = get_key_analytics(data)
    top_5_popular_artists = get_top_5_popular_artists(data)
    top_5_grossing_artists = get_top_5_grossing_artists(data)
    top_5_albums = get_top_5_sold_albums(data)
    top_5_tracks = get_top_5_sold_tracks(data)
    top_5_grossing_albums = get_top_5_grossing_albums(data)
    top_5_grossing_tracks = get_top_5_grossing_tracks(data)
    top_5_albums_list = top_5_albums['artist'].tolist()
    top_5_tracks_list = top_5_tracks['artist'].tolist()
    album_genres = get_album_genres(data, top_5_albums_list)
    track_genres = get_track_genres(data, top_5_tracks_list)
    top_genres = get_popular_genre(data)
    sales_per_country = get_countries_with_most_sales(data)
    artists_per_country = get_popular_artist_per_country(data)

    html_string = f"""
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <style> body {{
                background-color: powderblue;
                font-size: 16px;
                padding: 20px;
                }}
                h1 {{
                color: black;
                font-family: impact;
                font-size: 30px;
                text-align: center;
                }}
                h2 {{
                color: black;
                font-family: Arial Unicode MS;
                font-size: 20px;
                text-align: center;
                }}
                li {{
                font-family: Arial Unicode MS;
                font-size: 15px;
                text-align: center;
                list-style-type: none;
                }}
                p,a {{
                font-family: Arial Unicode MS;
                font-size: 15px;
                text-align: left;
                }}
                a {{
                color:#000080;
                text-decoration: none;
                }}
                table, th, td {{
                border: 1px solid white;
                border-collapse: collapse;
                width: 60%;
                background-color: #96D4D4;
                margin-top:15px;
                }}
                th, td {{
                padding: 8px;
                text-align: center;
                max-width: 200px;
                }}
                th{{
                background-color: #000080;
                color: white;
                }}

        </style>
    </head>
    <body>
        <h1> BandCamp Daily Report {YESTERDAY_DATE}</h1>
        <p> This is a daily report that contains the key analyses for bandcamp data from {YESTERDAY_DATE}.</p>
        <a  href="https://bandcamp.com/">Bandcamp website</a>
        <h2> Overview </h2>
        <li> Total Sales = {key_metrics[0]} </li>
        <li> Total Income = ${key_metrics[1]} </li>
        <h2> Top 5 Popular Artists  </h2>
        <table>
        <tr>
        <th> Artist</th>
        <th>Albums/Tracks Sold</th>
        </tr>
        <tr>
        <td>{top_5_popular_artists[0]['artist']}</td>
        <td>{top_5_popular_artists[0]['count']}</td>
        </tr>
        </table>
        <li> 1. {top_5_popular_artists[0]['artist']}: sold {top_5_popular_artists[0]['count']} items </li>
        <li> 2. {top_5_popular_artists[1]['artist']}: sold {top_5_popular_artists[1]['count']} items </li>
        <li> 3. {top_5_popular_artists[2]['artist']}: sold {top_5_popular_artists[2]['count']} items </li>
        <li> 4. {top_5_popular_artists[3]['artist']}: sold {top_5_popular_artists[3]['count']} items </li>
        <li> 5. {top_5_popular_artists[4]['artist']}: sold {top_5_popular_artists[4]['count']} items </li>
        <h2> Top 5 Grossing Artists </h2>
        <li> 1. {top_5_grossing_artists[0]['artist']}: made ${top_5_grossing_artists[0]['amount']} </li>
        <li> 2. {top_5_grossing_artists[1]['artist']}: made ${top_5_grossing_artists[1]['amount']} </li>
        <li> 3. {top_5_grossing_artists[2]['artist']}: made ${top_5_grossing_artists[2]['amount']} </li>
        <li> 4. {top_5_grossing_artists[3]['artist']}: made ${top_5_grossing_artists[3]['amount']} </li>
        <li> 5. {top_5_grossing_artists[4]['artist']}: made ${top_5_grossing_artists[4]['amount']} </li>
        <h2> Top 5 Popular Albums </h2>
        <li> 1. {top_5_albums[0]['item_name']}: sold {top_5_albums[0]['count']} copies </li>
        <li> 2. {top_5_albums[1]['item_name']}: sold {top_5_albums[1]['count']} copies </li>
        <li> 3. {top_5_albums[2]['item_name']}: sold {top_5_albums[2]['count']} copies </li>
        <li> 4. {top_5_albums[3]['item_name']}: sold {top_5_albums[3]['count']} copies </li>
        <li> 5. {top_5_albums[4]['item_name']}: sold {top_5_albums[4]['count']} copies </li>
        <h2> Top 5 Grossing Albums </h2>
        <li> 1. {top_5_grossing_albums[0]['item_name']}: made ${top_5_grossing_albums[0]['amount']} </li>
        <li> 2. {top_5_grossing_albums[1]['item_name']}: made ${top_5_grossing_albums[1]['amount']} </li>
        <li> 3. {top_5_grossing_albums[2]['item_name']}: made ${top_5_grossing_albums[2]['amount']} </li>
        <li> 4. {top_5_grossing_albums[3]['item_name']}: made ${top_5_grossing_albums[3]['amount']} </li>
        <li> 5. {top_5_grossing_albums[4]['item_name']}: made ${top_5_grossing_albums[4]['amount']} </li>
        <h2> Top 5 Popular Tracks </h2>
        <li> 1. {top_5_tracks[0]['item_name']}: sold {top_5_tracks[0]['count']} copies </li>
        <li> 2. {top_5_tracks[1]['item_name']}: sold {top_5_tracks[1]['count']} copies </li>
        <li> 3. {top_5_tracks[2]['item_name']}: sold {top_5_tracks[2]['count']} copies </li>
        <li> 4. {top_5_tracks[3]['item_name']}: sold {top_5_tracks[3]['count']} copies </li>
        <li> 5. {top_5_tracks[4]['item_name']}: sold {top_5_tracks[4]['count']} copies </li>
        <h2> Top 5 Grossing Tracks </h2>
        <li> 1. {top_5_grossing_tracks[0]['item_name']}: made ${top_5_grossing_tracks[0]['amount']} </li>
        <li> 2. {top_5_grossing_tracks[1]['item_name']}: made ${top_5_grossing_tracks[1]['amount']} </li>
        <li> 3. {top_5_grossing_tracks[2]['item_name']}: made ${top_5_grossing_tracks[2]['amount']} </li>
        <li> 4. {top_5_grossing_tracks[3]['item_name']}: made ${top_5_grossing_tracks[3]['amount']} </li>
        <li> 5. {top_5_grossing_tracks[4]['item_name']}: made ${top_5_grossing_tracks[4]['amount']} </li>
         <h2> Top 5 Genres </h2>
        <li> 1. {top_genres[0]['genre']}  </li>
        <li> 2. {top_genres[1]['genre']}  </li>
        <li> 3. {top_genres[2]['genre']}  </li>
        <li> 4. {top_genres[3]['genre']}  </li>
        <li> 5. {top_genres[4]['genre']} </li>
        </body>
    """

    return html_string


def convert_html_to_pdf(source_html, output_filename):
    # open output file for writing (truncated binary)
    result_file = open(output_filename, "w+b")

    # convert HTML to PDF
    pisa_status = pisa.CreatePDF(
        source_html,                # the HTML to convert
        dest=result_file)           # file handle to recieve result

    # close output file
    result_file.close()                 # close output file

    # return True on success and False on errors
    return pisa_status.err


def generate_pdf_report(data: pd.DataFrame):
    """Creates a pdf report """

    html_string = generate_html_string(data)
    convert_html_to_pdf(html_string, 'Bancamp-Daily-Report.pdf')

def handler(event=None, context=None) -> dict:
    """Handler for the lambda function"""

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)

    return {"pdf_report":}


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)
    top_5_albums = get_top_5_sold_albums(all_data)
    top_5_tracks = get_top_5_sold_tracks(all_data)
    top_5_grossing_albums = get_top_5_grossing_albums(all_data)
    top_5_grossing_tracks = get_top_5_grossing_tracks(all_data)
    top_5_albums_list = top_5_albums['artist'].tolist()
    top_5_tracks_list = top_5_tracks['artist'].tolist()
    album_genres = get_album_genres(all_data, top_5_albums_list)
    track_genres = get_track_genres(all_data, top_5_tracks_list)

    print(top_5_albums)
    print(album_genres)

    # html_report = generate_html_string(all_data)

    # generate_pdf_report(all_data)
