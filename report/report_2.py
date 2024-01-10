"""Script that creates a pdf daily report for the previous days sales data"""

from os import environ
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from dotenv import load_dotenv
from psycopg2 import extensions, connect
import pandas as pd
import boto3
from botocore.exceptions import ClientError
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


def load_sale_event_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

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
    amount_sales = sale_event_data['sale_id'].nunique()

    # amount income
    amount_income = (sale_event_data['amount'].sum())/100

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


def load_artist_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.sale_id, sale_event.amount, sale_event.item_id, artist.artist_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN artist
                    ON artist.artist_id = item.artist_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'amount', 'item_id',
                        'artist']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_top_5_popular_artists(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 most popular artists and how many items they sold
    """
    artist_data = load_artist_data(db_connection)

    popular_artists = artist_data['artist'].value_counts(
    ).sort_values(ascending=False).head(5).reset_index()

    artists = popular_artists.to_dict('records')

    html_string = create_table_two_columns(
        'Artist', 'Albums/Tracks Sold', artists, 'artist', 'count')

    return html_string


def get_top_5_grossing_artists(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 grossing artists and their total revenue
    """

    artist_data = load_artist_data(db_connection)

    artist_sales = artist_data.groupby(
        'artist')['amount'].sum()
    artist_sales = (
        artist_sales/100).sort_values(ascending=False).head(5).reset_index()

    artists = artist_sales.to_dict('records')

    html_string = create_table_two_columns(
        'Artist', 'Revenue', artists, 'artist', 'amount')

    return html_string


def load_album_track_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.sale_id, sale_event.amount, sale_event.item_id, item.item_name, item_type.item_type, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id =item_genre.genre_id
                    JOIN item_type
                    ON item_type.item_type_id = item.item_type_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'amount', 'item_id',
                        'item_name', 'item_type', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def get_top_5_sold_albums(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and how many copies they sold
    """
    album_data = load_album_track_data(db_connection)
    unique_sales = album_data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    popular_albums = album_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    albums = popular_albums.to_dict('records')

    html_string = create_table_two_columns(
        'Album', 'Copies Sold', albums, 'item_name', 'count')

    return html_string


def get_top_5_sold_tracks(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and how many copies they sold
    """
    track_data = load_album_track_data(db_connection)
    unique_sales = track_data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    popular_tracks = track_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()

    tracks = popular_tracks.to_dict('records')

    html_string = create_table_two_columns(
        'Tracks', 'Copies Sold', tracks, 'item_name', 'count')

    return html_string


def get_top_5_grossing_albums(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and their revenue
    """
    album_data = load_album_track_data(db_connection)
    unique_sales = album_data.drop_duplicates(subset='sale_id', keep='first')
    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    album_sales = album_sales.groupby(
        'item_name')['amount'].sum()
    album_sales = (
        album_sales/100).sort_values(ascending=False).head(5).reset_index()

    albums = album_sales.to_dict('records')

    html_string = create_table_two_columns(
        'Album', 'Revenue', albums, 'item_name', 'amount')

    return html_string


def get_top_5_grossing_tracks(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and their revenue
    """
    track_data = load_album_track_data(db_connection)
    unique_sales = track_data.drop_duplicates(subset='sale_id', keep='first')
    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    track_sales = track_sales.groupby(
        'item_name')['amount'].sum()
    track_sales = (
        track_sales/100).sort_values(ascending=False).head(5).reset_index()

    tracks = track_sales.to_dict('records')

    html_string = create_table_two_columns(
        'Tracks', 'Revenue', tracks, 'item_name', 'amount')

    return html_string


def get_album_genres(db_connection: extensions.connection) -> str:
    """
    Returns a html string of a table that contains information on
    the top 5 sold albums and how many copies they sold and their associated genres
    """

    album_data = load_album_track_data(db_connection)
    unique_sales = album_data.drop_duplicates(subset='sale_id', keep='first')

    album_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'track'].index)
    popular_albums = album_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()
    selected = popular_albums['item_name'].to_list()

    album_sales = album_data[album_data['item_type'] == 'album']

    filtered_album_sales = album_sales[album_sales['item_name'].isin(selected)]

    albums_genre = filtered_album_sales.groupby(['item_name'])[
        'genre'].agg(list).reset_index()

    albums_genre['genre'] = albums_genre['genre'].apply(
        lambda x: list(set(x))[:3])

    final = pd.merge(popular_albums, albums_genre).to_dict('records')

    html_string = create_table_three_columns(
        'Album', 'Copies Sold', 'Genres', final, 'item_name', 'count', 'genre')

    return html_string


def get_track_genres(db_connection: extensions.connection) -> pd.DataFrame:
    """
    Returns a html string of a table that contains information on
    the top 5 sold tracks and how many copies they sold and their associated genres
    """
    track_data = load_album_track_data(db_connection)
    unique_sales = track_data.drop_duplicates(subset='sale_id', keep='first')

    track_sales = unique_sales.drop(
        unique_sales[unique_sales['item_type'] == 'album'].index)
    popular_tracks = track_sales['item_name'].value_counts().sort_values(ascending=False).head(
        5).reset_index()
    selected = popular_tracks['item_name'].to_list()

    track_sales = track_data[track_data['item_type'] == 'track']

    filtered_track_sales = track_sales[track_sales['item_name'].isin(selected)]

    track_genre = filtered_track_sales.groupby(['item_name'])[
        'genre'].agg(list).reset_index()

    track_genre['genre'] = track_genre['genre'].apply(
        lambda x: list(set(x))[:3])

    final = pd.merge(popular_tracks, track_genre).to_dict('records')

    html_string = create_table_three_columns(
        'Track', 'Copies Sold', 'Genres', final, 'item_name', 'count', 'genre')

    return html_string


def load_genre_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

    with db_connection.cursor() as curr:

        curr.execute("""
                    SELECT sale_event.sale_id, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id =item_genre.genre_id
                    WHERE DATE(sale_time) = CURRENT_DATE - INTERVAL '1 day';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'genre']

        df = pd.DataFrame(tuples, columns=column_names)

        return df


def load_country_data(db_connection: extensions.connection) -> pd.DataFrame:
    """Loads all the data from yesterday from the database into a pandas dataframe"""

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
    unique_genre_count = genre_data['genre'].value_counts().head(
        5).reset_index()

    genres = unique_genre_count.to_dict('records')

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


# def handler(event=None, context=None):
#     """Handler for the lambda function"""

#     load_dotenv()

#     connection = get_db_connection()

#     sale_event = load_sale_event_data(connection)

#     html_string = generate_html_string()

#     pdf_file_path = '/tmp/Bandcamp-Daily-Report.pdf'

#     convert_html_to_pdf(html_string, pdf_file_path)
#     print("Report created.")

#     send_email(connection, pdf_file_path)
#     print("Email sent.")


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    html_report = generate_html_string(connection)

    pdf_file_path = './Bandcamp-Daily-Report.pdf'

    convert_html_to_pdf(html_report, pdf_file_path)
    # send_email(connection, pdf_file_path)
