"""Script that creates a pdf daily report for the previous days sales data"""

from os import environ
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from time import perf_counter
from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from xhtml2pdf import pisa


# pylint: disable=E1136

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


def create_table_two_columns(column_1: str, column_2: str, data: list[dict],
                             key: str, value: str) -> str:
    """
    Returns a html string of a table that contains two columns
    """
    html_string = f"<table><tr><th> {column_1} </th><th> {column_2}</th>"

    if column_2 == 'Revenue':
        for item in data:
            html_string += f"""
            <tr>
            <td>{item[f'{key}']}</td>
            <td>${item[f'{value}']}</td>
            </tr>"""

    else:
        for item in data:
            html_string += f"""
            <tr>
            <td>{item[f'{key}']}</td>
            <td>{item[f'{value}']}</td>
            </tr>"""

    html_string += "</table>"

    return html_string


def create_table_three_columns(column_1: str, column_2: str, column_3: str, data: list[dict],
                               key: str, value: str, value_2: str) -> str:
    """
    Returns a html string of a table that contains three columns
    """
    html_string = f"<table><tr><th> {column_1} </th><th> {column_2}</th><th> {column_3}</th>"

    for item in data:
        html_string += f"""
        <tr>
        <td>{item[f'{key}']}</td>
        <td>{item[f'{value}']}</td>
        <td>{item[f'{value_2}']}</td>
        </tr>"""

    html_string += "</table>"

    return html_string


def format_all_numbers(dictionaries: list[dict], key: str):
    """
    Formats numbers so that commas will be inserted where necessary
    e.g 1000 = 1,000
    """
    for dict in dictionaries:
        dict[f'{key}'] = '{:,}'.format(dict[f'{key}'])


def load_sale_event_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads all sale event data from the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_id, amount, item_id, country_id
                    FROM sale_event
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'amount', 'item_id',
                        'country_id']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_key_analytics(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the amount of sales and income.
    """

    sale_event_data = load_sale_event_data(db_connection)
    # amount number of sales
    amount_sales = ('{:,}'.format(sale_event_data['sale_id'].nunique()))

    # amount income
    amount_income = ('{:,}'.format((sale_event_data['amount'].sum())/100))

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


def load_top_5_popular_artist_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 artists that appear the most in the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT COUNT(sale_event.sale_id) AS count, artist.artist_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN artist
                    ON artist.artist_id = item.artist_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY artist.artist_name
                    ORDER BY count DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['count',
                        'artist']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_top_5_grossing_artist_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 artists that have made the most money from the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT SUM(sale_event.amount) AS amount, artist.artist_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN artist
                    ON artist.artist_id = item.artist_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY artist.artist_name
                    ORDER BY amount DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['amount',
                        'artist']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_top_5_popular_artists(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 most popular artists and how many items they sold
    """

    artist_data = load_top_5_popular_artist_data(db_connection)

    artists = artist_data.to_dict('records')

    format_all_numbers(artists, 'count')

    html_string = create_table_two_columns(
        'Artist', 'Albums/Tracks Sold', artists, 'artist', 'count')

    return html_string


def get_top_5_grossing_artists(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 grossing artists and their total revenue
    """

    artist_data = load_top_5_grossing_artist_data(db_connection)

    artist_data['amount'] = (
        artist_data['amount']/100)

    artists = artist_data.to_dict('records')

    format_all_numbers(artists, 'amount')

    html_string = create_table_two_columns(
        'Artist', 'Revenue', artists, 'artist', 'amount')

    return html_string


def load_album_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 albums that have sold the most copies from the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT COUNT(sale_event.sale_id) as count, item.item_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    AND item_type.item_type_id = 1
                    GROUP BY item.item_name
                    ORDER BY count DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['count', 'item_name']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_track_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 tracks that have sold the most copies from the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT COUNT(sale_event.sale_id) as count, item.item_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    AND item_type.item_type_id = 2
                    GROUP BY item.item_name
                    ORDER BY count DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['count', 'item_name']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_top_5_sold_albums(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and how many copies they sold
    """
    album_data = load_album_data(db_connection)

    albums = album_data.to_dict('records')

    format_all_numbers(albums, 'count')

    html_string = create_table_two_columns(
        'Album', 'Copies Sold', albums, 'item_name', 'count')

    return html_string


def get_top_5_sold_tracks(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and how many copies they sold
    """
    track_data = load_track_data(db_connection)

    tracks = track_data.to_dict('records')

    format_all_numbers(tracks, 'count')

    html_string = create_table_two_columns(
        'Tracks', 'Copies Sold', tracks, 'item_name', 'count')

    return html_string


def load_album_revenue_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 albums that have made the most money from the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT SUM(sale_event.amount) as amount, item.item_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    AND item_type.item_type_id = 1
                    GROUP BY item.item_name
                    ORDER BY amount DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['amount', 'item_name']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_track_revenue_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 tracks that have made the most money from the database into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT SUM(sale_event.amount) as amount, item.item_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    AND item_type.item_type_id = 2
                    GROUP BY item.item_name
                    ORDER BY amount DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['amount', 'item_name']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_top_5_grossing_albums(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and their revenue
    """
    album_data = load_album_revenue_data(db_connection)

    album_data['amount'] = (
        album_data['amount']/100)

    albums = album_data.to_dict('records')

    format_all_numbers(albums, 'amount')

    html_string = create_table_two_columns(
        'Album', 'Revenue', albums, 'item_name', 'amount')

    return html_string


def get_top_5_grossing_tracks(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and their revenue
    """
    track_data = load_track_revenue_data(db_connection)

    track_data['amount'] = (
        track_data['amount']/100)

    tracks = track_data.to_dict('records')

    format_all_numbers(tracks, 'amount')

    html_string = create_table_two_columns(
        'Tracks', 'Revenue', tracks, 'item_name', 'amount')

    return html_string


def load_album_genre_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads album data that also contains information about each genre associated
    with the album from the database and converts it to a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT COUNT(sale_event.sale_id) as count, item.item_name, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id = item_genre.genre_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    AND item_type.item_type_id = 1
                    GROUP BY item.item_name, genre.genre
                    ORDER BY count DESC;""")
        tuples = curr.fetchall()
        column_names = ['count', 'item_name', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_track_genre_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads track data that also contains information about each genre associated
    with the track from the database and converts it to a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT COUNT(sale_event.sale_id) as count, item.item_name, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id = item_genre.genre_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    AND item_type.item_type_id = 2
                    GROUP BY item.item_name, genre.genre
                    ORDER BY count DESC;""")
        tuples = curr.fetchall()
        column_names = ['count', 'item_name', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_album_genres(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and how many copies they sold and their associated genres
    """

    album_data = load_album_genre_data(db_connection)

    albums_genre = album_data.groupby(['item_name'])[
        'genre'].agg(list).reset_index()

    albums_genre['genre'] = albums_genre['genre'].apply(
        lambda x: list(set(x))[:3])

    album_data = album_data.drop('genre', axis=1)

    final = pd.merge(album_data, albums_genre).drop_duplicates(
        subset='item_name', keep='first').head(5).to_dict('records')

    format_all_numbers(final, 'count')

    html_string = create_table_three_columns(
        'Album', 'Copies Sold', 'Genres', final, 'item_name', 'count', 'genre')

    return html_string


def get_track_genres(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and how many copies they sold and their associated genres
    """
    track_data = load_track_genre_data(db_connection)

    track_genre = track_data.groupby(['item_name'])[
        'genre'].agg(list).reset_index()

    track_genre['genre'] = track_genre['genre'].apply(
        lambda x: list(set(x))[:3])

    track_data = track_data.drop('genre', axis=1)

    final = pd.merge(track_data, track_genre).drop_duplicates(
        subset='item_name', keep='first').head(5).to_dict('records')

    format_all_numbers(final, 'count')

    html_string = create_table_three_columns(
        'Track', 'Copies Sold', 'Genres', final, 'item_name', 'count', 'genre')

    return html_string


def load_genre_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads the top 5 genres that have sold the most copies from the database
    and converts it to a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT COUNT(sale_event.sale_id) as count, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id =item_genre.genre_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY genre.genre
                    ORDER BY count DESC
                    LIMIT 5;""")
        tuples = curr.fetchall()
        column_names = ['count', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_country_data(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Loads country data alongside the different artists selling in those countries
    from the database and converts it into a pandas dataframe
    """

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.sale_id, country.country, artist.artist_name
                    FROM sale_event
                    JOIN country
                    ON country.country_id = sale_event.country_id
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN artist
                    ON artist.artist_id = item.artist_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'country', 'artist']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_popular_genre(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    the top 5 genres and how many copies a genre has sold
    """
    genre_data = load_genre_data(db_connection)

    genres = genre_data.to_dict('records')

    format_all_numbers(genres, 'count')

    html_string = create_table_two_columns(
        'Genre', 'Copies Sold', genres, 'genre', 'count')

    return html_string


def get_countries_insights(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    how many sales have occurred in every country and who the countries top artist is
    """
    country_data = load_country_data(db_connection)
    country_sales = country_data['country'].value_counts(
    ).sort_values(ascending=False).reset_index()

    most_popular_artists = country_data.groupby('country')['artist'].apply(
        lambda x: x.value_counts().idxmax()).reset_index()

    final = pd.merge(country_sales, most_popular_artists).head(
        10).to_dict('records')

    format_all_numbers(final, 'count')

    html_string = create_table_three_columns(
        'Country', 'Number of Sales', 'Top Artist', final, 'country', 'count', 'artist')

    return html_string


def generate_html_string(db_connection: extensions.connection) -> str:
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
            <img class="header" src="./bandcamp_logo.jpeg"  style="width:200px;height:100px;">
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
            <img class="header" src="./bandcamp_logo.jpeg"  style="width:200px;height:100px;">
            <h2 class="header"> Key Metrics </h2>
            <p> This section delves into essential metrics that gauge the overall performance of the music marketplace on Bandcamp.
            It includes the total number of sales, indicating the volume of transaction and the total income generated, offering a financial perspective.
            </p>
            {get_key_analytics(db_connection)}
            <h2 class="header"> Top Performers </h2>
            <p> Discover the artists who stand out as top performers in the sales landscape.
                The report identifies the top 5 popular artists, showcasing those who have garnered the most attention, and the top 5 grossing artists,
                highlighting those who have achieved the highest revenue through their music.
            </p>
            <div class="row">
                <div class="column">
                    {get_top_5_popular_artists(db_connection)}
                </div>
                <br>  </br>
                <div class="column">
                    {get_top_5_grossing_artists(db_connection)}
                </div>
            </div>
        </div>
        <div class="new-page" class="footer">
            <img class="header" src="./bandcamp_logo.jpeg"  style="width:200px;height:100px;">
            <h2 class="header"> Sales Overview </h2>
            <p> Explore the top 5 albums and tracks, shedding light on the current preferences of Bandcamp users.
                This section provides an overview of the most popular music items, giving insights into customer choices and potential trends.
            </p>
            <h3 class="subtitle"> Albums </h3>
            <div class="row">
                <div class="column">
                    {get_top_5_sold_albums(db_connection)}
                </div>
                <div class="column">
                    {get_top_5_grossing_albums(db_connection)}
                </div>
            </div>
            <h3 class="subtitle"> Tracks </h3>
            <div class="row">
                <div class="column">
                    {get_top_5_sold_tracks(db_connection)}
                </div>
                <div class="column">
                    {get_top_5_grossing_tracks(db_connection)}
                </div>
            </div>
        </div>
        <div class="new-page" class="footer">
            <img class="header" src="./bandcamp_logo.jpeg"  style="width:200px;height:100px;">
            <h2 class="header"> Genre Analysis </h2>
            <p> Dive into the diverse world of music genres with detailed analysis.
                The report outlines the top 5 genres overall, the genres associated with the top 5 albums, and the genres of the top 5 tracks.
                This analysis aims to uncover patterns in genre preferences and potential areas for genre-specific marketing strategies.
            </p>
            {get_popular_genre(db_connection)}
            {get_album_genres(db_connection)}
            {get_track_genres(db_connection)}
        </div>
        <div class="new-page" class="footer">
            <img class="header" src="./bandcamp_logo.jpeg"  style="width:200px;height:100px;">
            <h2 class="header"> Regional Insights </h2>
            <p> Understand how music sales vary across different regions.
                Highlighting countries with the most sales provides valuable geographical insights.
                Additionally, identifying the most popular artists in each country offers a nuanced view of regional music preferences.
            </p>
            {get_countries_insights(db_connection)}
    </div>
    """
    return html_string


def convert_html_to_pdf(source_html, output_filename):
    """
    Converts a html string into a pdf.
    """

    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
        source_html,
        dest=result_file)

    result_file.close()

    return pisa_status.err


def load_subscribers(db_connection: extensions.connection) -> list[str]:
    """Loads all the subscriber emails from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""SELECT subscriber_email FROM subscribers;""")
        tuples = curr.fetchall()
        subscribers = []
        for tuple in tuples:
            subscribers.append(tuple[0])

        return subscribers


def send_email(db_connection: extensions.connection, report_file_path: str):
    """
    Attaches the pdf to an email and sends the email
    """

    client = boto3.client("ses",
                          region_name="eu-west-2",
                          aws_access_key_id=environ["AWS_ACCESS_KEY_ID_"],
                          aws_secret_access_key=environ["AWS_SECRET_ACCESS_KEY_"])
    message = MIMEMultipart()
    message["Subject"] = "Bandcamp Daily Report"

    attachment = MIMEApplication(open(report_file_path, 'rb').read())
    attachment.add_header('Content-Disposition',
                          'attachment', filename='Bandcamp-Daily-Report.pdf')
    message.attach(attachment)

    subscribers = load_subscribers(db_connection)
    for subscriber in subscribers:
        try:
            client.send_raw_email(
                Source='trainee.ishika.madhav@sigmalabs.co.uk',
                Destinations=[subscriber],
                RawMessage={
                    'Data': message.as_string()
                }
            )

        except ClientError:
            continue


def handler(event=None, context=None):
    """Handler for the lambda function"""

    load_dotenv()

    connection = get_db_connection()

    html_string = generate_html_string(connection)

    pdf_file_path = '/tmp/Bandcamp-Daily-Report.pdf'

    convert_html_to_pdf(html_string, pdf_file_path)
    print("Report created.")

    send_email(connection, pdf_file_path)
    print("Email sent.")


if __name__ == "__main__":

    t1_start = perf_counter()

    load_dotenv()

    connection = get_db_connection()

    html_report = generate_html_string(connection)

    pdf_file_path = './Bandcamp-Daily-Report.pdf'

    convert_html_to_pdf(html_report, pdf_file_path)

    t1_stop = perf_counter()
    print("Elapsed time report 2 during the whole program in seconds:",
          t1_stop-t1_start)

    send_email(connection, pdf_file_path)
