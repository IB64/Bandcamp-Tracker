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
        return None


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


def get_key_analytics(data: pd.DataFrame) -> str:
    """Returns the amount sales and the amount income for the day"""

    # amount number of sales
    amount_sales = data['sale_id'].nunique()

    # amount income
    amount_income = (data['amount'].sum())/100

    html_string = f"""<table class="center">
            <tr>
            <th> Total Items Sold</th>
            <th>Total Income</th>
            </tr>
            <tr>
        <td>{amount_sales}</td>
            <td>${amount_income}</td>
        </tr>
    </table>"""

    return html_string


def get_top_5_popular_artists(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 most popular artists and how many items they sold
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    popular_artists = unique_sales['artist'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    artists = popular_artists.to_dict('records')

    html_string = "<table><tr><th> Artist </th><th> Albums/Tracks Sold</th>"

    for artist in artists:
        html_string += f"""
        <tr>
           <td>{artist['artist']}</td>
           <td>{artist['count']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_top_5_grossing_artists(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 grossing artists and their total revenue
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    artist_sales = unique_sales.groupby(
        'artist')['amount'].sum()
    artist_sales = (
        artist_sales/100).sort_values(ascending=False).head(5).reset_index()

    artists = artist_sales.to_dict('records')

    html_string = "<table> <tr> <th> Artist </th> <th> Revenue </th>"

    for artist in artists:
        html_string += f"""
        <tr>
           <td>{artist['artist']}</td>
           <td>${artist['amount']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_top_5_sold_albums(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and how many copies they sold
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    popular_albums = album_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    albums = popular_albums.to_dict('records')

    html_string = "<table> <tr> <th> Album </th> <th> Copies Sold </th>"

    for album in albums:
        html_string += f"""
        <tr>
           <td>{album['item_name']}</td>
           <td>{album['count']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_top_5_sold_tracks(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and how many copies they sold
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    popular_tracks = track_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    tracks = popular_tracks.to_dict('records')

    html_string = "<table> <tr> <th> Tracks </th> <th> Copies Sold </th>"

    for track in tracks:
        html_string += f"""
        <tr>
           <td>{track['item_name']}</td>
           <td>{track['count']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_top_5_grossing_albums(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and their revenue
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    album_sales = album_sales.groupby(
        'item_name')['amount'].sum()
    album_sales = (
        album_sales/100).sort_values(ascending=False).head(5).reset_index()

    albums = album_sales.to_dict('records')

    html_string = "<table> <tr> <th> Album </th> <th> Revenue </th>"

    for album in albums:
        html_string += f"""
        <tr>
        <td>{album['item_name']}</td>
        <td>${album['amount']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_top_5_grossing_tracks(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and their revenue
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    track_sales = track_sales.groupby(
        'item_name')['amount'].sum()
    track_sales = (
        track_sales/100).sort_values(ascending=False).head(5).reset_index()

    tracks = track_sales.to_dict('records')

    html_string = "<table> <tr> <th> Tracks </th> <th> Revenue </th>"

    for track in tracks:
        html_string += f"""
        <tr>
        <td>{track['item_name']}</td>
        <td>${track['amount']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_album_genres(data: pd.DataFrame) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and how many copies they sold and their associated genres
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')

    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    popular_albums = album_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()
    selected = popular_albums['item_name'].to_list()

    album_sales = data[data['item_type'] == 'album']

    filtered_album_sales = album_sales[album_sales['item_name'].isin(selected)]

    albums_genre = filtered_album_sales.groupby(['item_name'])[
        'genre'].agg(list).reset_index()

    albums_genre['genre'] = albums_genre['genre'].apply(
        lambda x: list(set(x)))

    final = pd.merge(popular_albums, albums_genre).to_dict('records')

    html_string = "<table> <tr> <th> Album </th> <th> Copies Sold </th> <th> Genres </th>"

    for album in final:
        html_string += f"""
        <tr>
        <td>{album['item_name']}</td>
        <td>{album['count']}</td>
        <td>{album['genre'][:3]}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_track_genres(data: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and how many copies they sold and their associated genres
    """

    unique_sales = data.drop_duplicates(subset='sale_id', keep='first')

    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    popular_tracks = track_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()
    selected = popular_tracks['item_name'].to_list()

    track_sales = data[data['item_type'] == 'track']

    filtered_track_sales = track_sales[track_sales['item_name'].isin(selected)]

    track_genre = filtered_track_sales.groupby(['item_name'])[
        'genre'].agg(list).reset_index()

    track_genre['genre'] = track_genre['genre'].apply(
        lambda x: list(set(x)))

    final = pd.merge(popular_tracks, track_genre).to_dict('records')

    html_string = "<table> <tr> <th> Track </th> <th> Copies Sold </th> <th> Genres </th>"

    for track in final:
        html_string += f"""
        <tr>
        <td>{track['item_name']}</td>
        <td>{track['count']}</td>
        <td>{track['genre'][:3]}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_popular_genre(data: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    the top 5 genres and how many copies a genre has sold
    """

    unique_genre_count = data['genre'].value_counts().head(5).reset_index()

    genres = unique_genre_count.to_dict('records')

    html_string = "<table> <tr> <th> Genre </th> <th> Copies Sold </th>"

    for genre in genres:
        html_string += f"""
        <tr>
        <td>{genre['genre']}</td>
        <td>{genre['count']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def get_countries_insights(data: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    how many sales have occurred in every country and who the countries top artist is
    """

    country_sales = data['country'].value_counts(
    ).sort_values(ascending=False).reset_index()

    most_popular_artists = data.groupby('country')['artist'].apply(
        lambda x: x.value_counts().idxmax()).reset_index()

    final = pd.merge(country_sales, most_popular_artists).to_dict('records')

    html_string = "<table><tr><th> Country </th><th> Number of Sales </th><th> Top Artist </th>"

    for country in final:
        html_string += f"""
        <tr>
        <td>{country['country']}</td>
        <td>{country['count']}</td>
        <td>{country['artist']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def generate_html_string(data: pd.DataFrame) -> str:
    """Returns a html string that contains all the daily report analyses"""

    html_string = f"""
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=210mm, height=297mm", initial-scale=1.0">
        <link rel="stylesheet" href="static/style.css">
        </head>
    <body>
        <div>
            <img class=title src="./bandcamp_logo.jpeg">
            <h1 class=title> Bandcamp Report </h1>
            <h1 class=date> {YESTERDAY_DATE} </h1>
        </div>
        <div class="new-page", class="footer">
            <h2 class=header> Overview </h2>
            <p> This is a daily report that contains the key analyses for <a href="https://bandcamp.com/">Bandcamp</a> data from {YESTERDAY_DATE}.
                <br> The Bandcamp Tracker Report offers a detailed exploration of sales data, providing valuable insights into the music industry's dynamics.
                    By analysing data from {YESTERDAY_DATE}, the report aims to offer a snapshot of trends and patterns in music purchases as well as trending genres and regional data
            </p>
            <h2 class=header> Contents </h2>
            <p class=contents>
                <br> Key Metrics </br>
                <br> Top Performers </br>
                <br> Sales Overview </br>
                <br> Genre Analysis </br>
                <br> Regional Analysis </br>
            </p>
        </div>
        <div class="new-page" class="footer">
            <h2 class="header"> Key Metrics </h2>
            <p> This section delves into essential metrics that gauge the overall performance of the music marketplace on Bandcamp.
            It includes the total number of sales, indicating the volume of transaction and the total income generated, offering a financial perspective.
            </p>
            {get_key_analytics(data)}
            <h2 class="header"> Top Performers </h2>
            <p> Discover the artists who stand out as top performers in the sales landscape.
                The report identifies the top 5 popular artists, showcasing those who have garnered the most attention, and the top 5 grossing artists,
                highlighting those who have achieved the highest revenue through their music.
            </p>
            <div class="row">
                <div class="column">
                    {get_top_5_popular_artists(data)}
                </div>
                <div class="column">
                    {get_top_5_grossing_artists(data)}
                </div>
            </div>
        </div>
        <div class="new-page" class="footer">
            <h2 class="header"> Sales Overview </h2>
            <p> Explore the top 5 albums and tracks, shedding light on the current preferences of Bandcamp users.
                This section provides an overview of the most popular music items, giving insights into customer choices and potential trends.
            </p>
            <h3 class="subtitle"> Albums </h3>
            <div class="row">
                <div class="column">
                    {get_top_5_sold_albums(data)}
                </div>
                <div class="column">
                    {get_top_5_grossing_albums(data)}
                </div>
            </div>
            <h3 class="subtitle"> Tracks </h3>
            <div class="row">
                <div class="column">
                    {get_top_5_sold_tracks(data)}
                </div>
                <div class="column">
                    {get_top_5_grossing_tracks(data)}
                </div>
            </div>
        </div>
        <div class="new-page" class="footer">
            <h2 class="header"> Genre Analysis </h2>
            <p> Dive into the diverse world of music genres with detailed analysis.
                The report outlines the top 5 genres overall, the genres associated with the top 5 albums, and the genres of the top 5 tracks.
                This analysis aims to uncover patterns in genre preferences and potential areas for genre-specific marketing strategies.
            </p>
            {get_popular_genre(data)}
            {get_album_genres(data)}
            {get_track_genres(data)}
        </div>
        <div class="new-page" class="footer">
            <h2 class="header"> Regional Insights </h2>
            <p> Understand how music sales vary across different regions.
                Highlighting countries with the most sales provides valuable geographical insights.
                Additionally, identifying the most popular artists in each country offers a nuanced view of regional music preferences.
            </p>
            {get_countries_insights(data)}
    </div>
    """
    return html_string


def convert_html_to_pdf(source_html, output_filename):
    """
    Converts a html string into a pdf.
    """
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


def handler(event=None, context=None) -> dict:
    """Handler for the lambda function"""

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)

    html_string = generate_html_string(all_data)

    pdf_file_path = '/tmp/Bandcamp-Daily-Report.pdf'
    convert_html_to_pdf(html_string, pdf_file_path)

    # Return the file path
    return {"pdf_report_path": pdf_file_path}


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)

    html_string = generate_html_string(all_data)

    pdf_file_path = './Bandcamp-Daily-Report.pdf'
    convert_html_to_pdf(html_string, pdf_file_path)
