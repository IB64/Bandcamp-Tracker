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

    # key_metrics = get_key_analytics(data)
    # top_5_popular_artists = get_top_5_popular_artists(data)
    # top_5_grossing_artists = get_top_5_grossing_artists(data)
    # top_5_albums = get_top_5_sold_albums(data)
    # top_5_tracks = get_top_5_sold_tracks(data)
    # top_5_grossing_albums = get_top_5_grossing_albums(data)
    # top_5_grossing_tracks = get_top_5_grossing_tracks(data)
    # # top_5_albums_list = top_5_albums['artist'].tolist()
    # # top_5_tracks_list = top_5_tracks['artist'].tolist()
    # # album_genres = get_album_genres(data, top_5_albums_list)
    # # track_genres = get_track_genres(data, top_5_tracks_list)
    # top_genres = get_popular_genre(data)
    # sales_per_country = get_countries_with_most_sales(data)
    # artists_per_country = get_popular_artist_per_country(data)

    html_string = """
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=210mm, height=297mm", inital-scale=1.0">
    <link rel="stylesheet" href="static/style.css">
    </head>
    <body>
    <div>
    <img class=title src="./bandcamp_logo.jpeg">
    <h1 class=title> Bandcamp Report </h1>
    <h1 class=date> 03-01-24 </h1>
    </div>
    <div class=new-page>
    <h2 class=header> Overview </h2>
    <p> This is a daily report that contains the key analyses for <a href="https://bandcamp.com/">Bandcamp</a> data from 03-01-24.
    <br> The Bandcamp Tracker Report offers a detailed exploration of sales data, providing valuable insights into the music industry's dynamics. By analysing data from 03-01-24, the report aims to offer a snapshot of trends and patterns in music purchases as well as trending genres and regional data.</p>
    <h2 class=header> Contents </h2>
    <p class=contents>
    <br> Key Metrics.......3 </br>
    <br> Top Performers.......3 </br>
    <br> Sales Overview.......4 </br>
    <br> Genre Analysis.......5 </br>
    <br> Regional Analysis.......6 </br> </p>
    </div>
    <div class="new-page">
    <h2 class="header"> Key Metrics </h2>
    <p> This section delves into essential metrics that gauge the overall performance of the music marketplace on Bandcamp. It includes the total number of sales, indicating the volume of transaction and the total income generated, offering a finiancial perspective. </p>
    <table class="center">
        <tr>
        <th> Total Items Sold</th>
        <th>Total Income</th>
        </tr>
        <tr>
        <td>{key_metrics[0]}</td>
            <td>${key_metrics[1]}</td>
        </tr>
    </table>
    <h2 class="header"> Top Performers </h2>
  <p> Discover the artists who stand out as top performers in the sales landscape. The report identifies the top 5 popular artists, showcasing those who have garnered the most attention, and the top 5 grossing artists, highlighting those who have achieved the highest revenue through their music.
    <div class="row">
    <div class="column">
  <table>
    <tr>
        <th> Artist </th>
        <th> Albums/Tracks Sold</th>
    </tr>
    <tr>
       <td>{top_5_popular_artists[0]['artist']}</td>
       <td>{top_5_popular_artists[0]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_popular_artists[1]['artist']}</td>
       <td>{top_5_popular_artists[1]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_popular_artists[2]['artist']}</td>
       <td>{top_5_popular_artists[2]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_popular_artists[3]['artist']}</td>
       <td>{top_5_popular_artists[3]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_popular_artists[4]['artist']}</td>
       <td>{top_5_popular_artists[4]['count']}</td>
    </tr>
  </table>
</div>
<div class="column">
  <table>
    <tr>
        <th> Artist </th>
        <th> Revenue </th>
    </tr>
    <tr>
       <td>{top_5_grossing_artists[0]['artist']}</td>
       <td>${top_5_grossing_artists[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_grossing_artists[1]['artist']}</td>
       <td>${top_5_grossing_artists[1]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_grossing_artists[2]['artist']}</td>
       <td>${top_5_grossing_artists[2]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_grossing_artists[3]['artist']}</td>
       <td>${top_5_grossing_artists[3]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_grossing_artists[4]['artist']}</td>
       <td>${top_5_grossing_artists[4]['amount']}</td>
    </tr>
  </table>
</div>
</div>
</div>
<div class="new-page">
<h2 class="header"> Sales Overview </h2>
<p> Explore the top 5 albums and tracks, shedding light on the current preferences of Bandcamp users. This section provides an overview of the most popular music items, giving insights into customer choices and potential trends. <p>
<h3 class="subtitle"> Albums </h3>
 <div class="row">
    <div class="column">
  <table>
    <tr>
        <th> Album </th>
        <th> Copies Sold </th>
    </tr>
    <tr>
       <td>{top_5_albums[0]['item_name']}</td>
       <td>{top_5_albums[0]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[1]['item_name']}</td>
       <td>{top_5_albums[1]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[2]['item_name']}</td>
       <td>{top_5_albums[2][counte']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[3]['item_name']}</td>
       <td>{top_5_albums[3]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[4]['item_name']}</td>
       <td>{top_5_albums[4]['count']}</td>
    </tr>
  </table>
</div>
<div class="column">
  <table>
    <tr>
        <th> Album </th>
        <th> Revenue </th>
    </tr>
    <tr>
       <td>{top_5_albums[0]['item_name']}</td>
       <td>${top_5_albums[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[0]['item_name']}</td>
       <td>${top_5_albums[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[0]['item_name']}</td>
       <td>${top_5_albums[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[0]['item_name']}</td>
       <td>${top_5_albums[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_albums[0]['item_name']}</td>
       <td>${top_5_albums[0]['amount']}</td>
    </tr>
  </table>
</div>
</div>
<h3 class="subtitle"> Tracks </h3>
 <div class="row">
    <div class="column">
  <table>
    <tr>
        <th> Track </th>
        <th> Copies Sold </th>
    </tr>
    <tr>
       <td>{top_5_tracks[0]['item_name']}</td>
       <td>{top_5_tracks[0]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[1]['item_name']}</td>
       <td>{top_5_tracks[1]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[2]['item_name']}</td>
       <td>{top_5_tracks[2]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[3]['item_name']}</td>
       <td>{top_5_tracks[3]['count']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[4]['item_name']}</td>
       <td>{top_5_tracks[4]['count']}</td>
    </tr>
  </table>
</div>
<div class="column">
  <table>
    <tr>
        <th> Tracks </th>
        <th> Revenue </th>
    </tr>
    <tr>
       <td>{top_5_tracks[0]['item_name']}</td>
       <td>${top_5_tracks[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[0]['item_name']}</td>
       <td>${top_5_tracks[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[0]['item_name']}</td>
       <td>${top_5_tracks[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[0]['item_name']}</td>
       <td>${top_5_tracks[0]['amount']}</td>
    </tr>
     <tr>
       <td>{top_5_tracks[0]['item_name']}</td>
       <td>${top_5_tracks[0]['amount']}</td>
    </tr>
  </table>
</div>
</div>
</div>
<div class="new-page">
<h2 class="header"> Genre Analysis </h2>
<p> Dive into the diverse world of music genres with detailed analysis. The report outlines the top 5 genres overall, the genres associated with the top 5 albums, and the genres of the top 5 tracks. This analysis aims to uncover patterns in genre preferences and potential areas for genre-specific marketing strategies. <p>
 <table class="center">
        <tr>
        <th> Genre </th>
        <th>Copies Sold</th>
        </tr>
        <tr>
        <td>{top_genres[0]['genre']}</td>
        <td>{key_metrics[1]}</td>
        </tr>
        <tr>
        <td>{top_genres[1]['genre']}</td>
            <td>{key_metrics[1]}</td>
        </tr>
        <tr>
        <td>{top_genres[2]['genre']}</td>
            <td>{key_metrics[1]}</td>
        </tr>
        <tr>
        <td>{top_genres[3]['genre']}</td>
            <td>{key_metrics[1]}</td>
        </tr>
        <tr>
        <td>{top_genres[4]['genre']}</td>
            <td>{key_metrics[1]}</td>
        </tr>
    </table>
 <table class="center">
        <tr>
        <th> Album </th>
        <th>Copies Sold</th>
        <th>Genre</th>
        </tr>
        <tr>
        <td>album 0</td>
        <td>sold 0</td>
        <td>genres 0</td>
        </tr>
        <tr>
        <td>album 1</td>
        <td>sold 1</td>
        <td>genres 1</td>
        </tr>
        <tr>
        <td>album 2</td>
        <td>sold 2</td>
        <td>genres 2</td>
        </tr>
        <tr>
        <td>album 3</td>
        <td>sold 3</td>
        <td>genres 3</td>
        </tr>
        <tr>
        <td>album 4</td>
        <td>sold 4</td>
        <td>genres 4</td>
        </tr>
    </table>
 <table class="center">
        <tr>
        <th> Tracks </th>
        <th>Copies Sold</th>
        <th>Genre</th>
        </tr>
        <tr>
        <td>Track 0</td>
        <td>sold 0</td>
        <td>genres 0</td>
        </tr>
        <tr>
        <td>Track 1</td>
        <td>sold 1</td>
        <td>genres 1</td>
        </tr>
        <tr>
        <td>Track 2</td>
        <td>sold 2</td>
        <td>genres 2</td>
        </tr>
        <tr>
        <td>Track 3</td>
        <td>sold 3</td>
        <td>genres 3</td>
        </tr>
        <tr>
        <td>Track 4</td>
        <td>sold 4</td>
        <td>genres 4</td>
        </tr>
    </table>
    </div>
    <div class="new-page">
    <h2 class="header"> Regional Insights </h2>
    <p> Understand how music sales vary across different regions. Highlighting countries with the most sales provides valuable geographical insights. Additionally, identifying the most popular artists in each country offers a nuanced view of regional music preferences <p>
    <table class="center">
        <tr>
        <th> Country </th>
        <th> Sales </th>
        <th> Top Artist</th>
        </tr>
    </table>
    </div>
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

# def handler(event=None, context=None) -> dict:
#     """Handler for the lambda function"""

#     load_dotenv()

#     connection = get_db_connection()

#     all_data = load_all_data(connection)

#     return {"pdf_report":}


if __name__ == "__main__":

    load_dotenv()

    connection = get_db_connection()

    all_data = load_all_data(connection)
    top_5_albums = get_top_5_sold_albums(all_data)
    top_5_tracks = get_top_5_sold_tracks(all_data)
    top_5_grossing_albums = get_top_5_grossing_albums(all_data)
    top_5_grossing_tracks = get_top_5_grossing_tracks(all_data)
    # top_5_albums_list = top_5_albums['item_name'].tolist()
    # top_5_tracks_list = top_5_tracks['item_name'].tolist()
    # album_genres = get_album_genres(all_data, top_5_albums_list)
    # track_genres = get_track_genres(all_data, top_5_tracks_list)

    print(all_data)

    html_report = generate_html_string(all_data)

    generate_pdf_report(all_data)
