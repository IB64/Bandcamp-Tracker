"""Live analytics page for the StreamLit dashboard, showing live graph visualisations."""
from datetime import datetime
from os import environ
from requests import get

from dotenv import load_dotenv
import pandas as pd

import altair as alt
from psycopg2 import extensions, connect
import streamlit as st
from vega_datasets import data

# pylint: disable=E1136


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


@st.cache_data
def load_sales_data(_db_connection: extensions.connection, start_time, end_time) -> pd.DataFrame:
    """Loads all the artist, track and album sale data in a given timeframe."""
    with _db_connection.cursor() as curr:
        curr.execute(f"""
                    SELECT sale_event.sale_id, sale_event.sale_time, item.item_name, item.item_type_id, artist.artist_name
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN artist
                    ON artist.artist_id = item.artist_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    WHERE sale_event.sale_time >= '{start_time}'
                    AND sale_event.sale_time <= '{end_time}';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time',
                        'item_name', 'item_type', 'artist']
        return pd.DataFrame(tuples, columns=column_names)


@st.cache_data
def load_sales_genres(_db_connection: extensions.connection, start_time, end_time) -> pd.DataFrame:
    """Loads all the genre sale data in a given timeframe."""
    with _db_connection.cursor() as curr:
        curr.execute(f"""
                    SELECT sale_event.sale_id, sale_event.sale_time, item.item_name, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id = item_genre.genre_id
                    WHERE sale_event.sale_time >= '{start_time}' 
                    AND sale_event.sale_time <= '{end_time}';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time',
                        'item_name', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


@st.cache_data
def get_specific_track_data(_db_connection, start_time, end_time, track_name) -> pd.DataFrame:
    """Loads the artist, album, genre sale data for a given track or album in a given timeframe."""
    with _db_connection.cursor() as curr:
        curr.execute(f"""
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
                    ON genre.genre_id = item_genre.genre_id
                    WHERE item.item_name = '{track_name}'
                    AND sale_event.sale_time >= '{start_time}' 
                    AND sale_event.sale_time <= '{end_time}';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'item_id', 'country_id',
                        'country', 'item_name', 'item_type',
                        'item_image', 'artist', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


@st.cache_data
def get_specific_artist_data(_db_connection: extensions.connection, start_time, end_time, artist_name) -> pd.DataFrame:
    """Loads all the artist, album, genre sale data for a given artist in a given timeframe."""
    with _db_connection.cursor() as curr:
        curr.execute(f"""
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
                    ON genre.genre_id = item_genre.genre_id
                    WHERE artist.artist_name = '{artist_name}'
                    AND sale_event.sale_time >= '{start_time}' 
                    AND sale_event.sale_time <= '{end_time}';""")
        tuples = curr.fetchall()
        column_names = ['sale_id', 'sale_time', 'amount', 'item_id', 'country_id',
                        'country', 'item_name', 'item_type',
                        'item_image', 'artist', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


@st.cache_data
def load_genre_and_countries(_db_connection, start_time, end_time) -> pd.DataFrame:
    """Loads the genre and country data required for the heat map."""
    with _db_connection.cursor() as curr:
        curr.execute(f"""
                    SELECT country.country, genre.genre
                    FROM sale_event
                    JOIN item
                    ON item.item_id = sale_event.item_id
                    JOIN item_genre
                    ON item_genre.item_id = item.item_id
                    JOIN genre
                    ON genre.genre_id = item_genre.genre_id
                    JOIN country
                    ON country.country_id = sale_event.country_id
                    WHERE sale_event.sale_time >= '{start_time}' 
                    AND sale_event.sale_time <= '{end_time}';""")
        tuples = curr.fetchall()
        column_names = ['country', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


def build_date_range_slider() -> tuple[datetime, datetime]:
    """Creates a date range selector for user to choose a date range."""

    end_date = datetime.utcnow()
    start_date = end_date - pd.Timedelta(days=1)

    start_date = st.date_input("Select start date:", start_date)
    end_date = st.date_input("Select end date:", end_date)

    return start_date, end_date


def create_sales_chart(df: pd.DataFrame, object_type: str) -> None:
    """Creates the line graph showing sales of artists/genres/tracks over time."""

    if object_type != 'item_name':
        suggestions = set(list(df[f'{object_type}']))
        selections = st.multiselect(
            f"Select {object_type.title()}s", suggestions)

        chart_title = F'Sales Over Time - Top {object_type.title()}s'

    else:
        suggestions = set([
            f"{track} (Track)" if item_type == 1 else f"{track} (Album)"
            for track, item_type in zip(df[f'{object_type}'], df['item_type'])
        ])

        selected_tracks = st.multiselect(
            "Select Track/Albums", suggestions)

        selections = [track.split(
            " (")[0] for track in selected_tracks]

        chart_title = 'Sales Over Time - Top Tracks/Albums'

    if selections:
        selected_df = df[df[f'{object_type}'].isin(selections)]
        chart_title = 'Sales Over Time -'
        for item_name in selections:
            if len(chart_title) == 17:
                chart_title += f' {item_name}'
            else:
                chart_title += f', {item_name}'
    else:
        tops = df[f'{object_type}'].value_counts().head(5).index.tolist()
        selected_df = df[df[f'{object_type}'].isin(tops)]

    selected_df['sale_time'] = pd.to_datetime(selected_df['sale_time'])

    grouped_data = selected_df.groupby([
        pd.Grouper(key='sale_time', freq='D'),
        f'{object_type}'
    ])[f'{object_type}'].count().reset_index(name='total')

    artist_chart = alt.Chart(grouped_data).mark_line().encode(
        x=alt.X('sale_time:T', title='Time'),
        y=alt.Y('total:Q', title='Number of Copies Sold'),
        color=alt.Color(f'{object_type}:N', scale=alt.Scale(scheme='blues')),
        detail=f'{object_type}:N'
    ).properties(
        title=chart_title
    ).configure_title(
        anchor='middle')

    st.altair_chart(artist_chart, use_container_width=True)


@st.cache_data
def create_country_graph(df: pd.DataFrame) -> None:
    """Creates a bar chart showing the number of sales for each country."""
    total_sales = df.groupby('country').size().reset_index(name='count')
    top_items = total_sales.nlargest(5, 'count')
    chart = alt.Chart(top_items).mark_bar().encode(
        x=alt.X('country', title='Country'),
        y=alt.Y('count', title='Number of Copies Sold'),
        color=alt.Color('country:N', scale=alt.Scale(scheme='blues'))
    ).properties(
        title='Sales in Top 5 Countries',
        width=600,
        height=400
    ).configure_title(
        anchor='middle'
    ).configure_legend(
        disable=True)
    st.altair_chart(chart, use_container_width=True)


@st.cache_data
def create_price_graph(df: pd.DataFrame) -> None:
    """Creates a line graph showing the number of sales over time."""
    print(df)
    # df['sale_time'] = pd.to_datetime(df['sale_time'])
    total_sales = df.groupby(
        df['sale_time'].dt.hour).size().reset_index(name='count')
    chart = alt.Chart(total_sales).mark_line().encode(
        x=alt.X('sale_time:T', title='Time'),
        y=alt.Y('count', title='Number of Copies Sold'),
    ).properties(
        title='Sales in the last 5 Days',
        width=400,
        height=300
    ).configure_title(
        anchor='middle'
    )
    st.altair_chart(chart, use_container_width=True)


@st.cache_data
def create_album_track_graph(df: pd.DataFrame) -> None:
    """Creates a bar chart showing the number of sales for each album/track an artist has."""
    total_sales = df.groupby('item_name').size().reset_index(name='count')
    top_items = total_sales.nlargest(10, 'count')

    chart = alt.Chart(top_items).mark_bar().encode(
        x=alt.X('item_name', title='Track/Album'),
        y=alt.Y('count', title='Number of Copies sold'),
        color=alt.Color('item_name:N', scale=alt.Scale(scheme='blues'))
    ).properties(
        title='Top Track/Album Sales',
        width=600,
        height=400
    ).configure_legend(
        disable=True
    ).configure_title(
        anchor='middle'
    )
    st.altair_chart(chart, use_container_width=True)


@st.cache_data
def create_heat_map_graph(all_genres_df: pd.DataFrame, genre: str, select: str) -> None:
    """Creates a country-genre heat map, showing where genres are most popular in the world."""

    genre_filtered_data = all_genres_df[all_genres_df['genre'] == genre.lower(
    )]

    total_country_counts = all_genres_df['country'].value_counts(
    ).reset_index(name='total')

    country_count_df = genre_filtered_data['country'].value_counts(
    ).reset_index(name='popularity')

    country_count_df = country_count_df.merge(
        total_country_counts, left_on='country', right_on='country', how='inner'
    )

    country_count_df['percentage'] = round((country_count_df['popularity'] /
                                            country_count_df['total'] * 100), 2)

    country_count_df['country'] = country_count_df['country'].replace(
        'United Kingdom', 'United Kingdom of Great Britain and Northern Ireland')
    country_count_df['country'] = country_count_df['country'].replace(
        'United States', 'United States of America')

    source = alt.topo_feature(data.world_110m.url, "countries")

    country_codes = pd.read_csv("country_codes.csv")

    background = alt.Chart(source).mark_geoshape(fill="white")

    if select == 'Percentage':
        select = 'percentage'
        title = 'Total Percentage'
    if select == 'Total Count':
        select = 'popularity'
        title = 'Genre Count'

    foreground = alt.Chart(source).mark_geoshape(
        stroke="black", strokeWidth=0.15
    ).encode(
        color=alt.Color(
            f"{select}:N", scale=alt.Scale(scheme="blues"), legend=None,
        ),
        tooltip=[
            alt.Tooltip("name:N", title="Country"),
            alt.Tooltip(f"{select}:Q", title=title),
        ],
    ).transform_lookup(
        lookup="id",
        from_=alt.LookupData(data=country_codes,
                             key="country-code", fields=["name"]),
    ).transform_lookup(
        lookup='name',
        from_=alt.LookupData(country_count_df, 'country', [
            f'{select}'])
    )

    final_map = ((background + foreground).configure_view(strokeWidth=0).properties(
        width=500).project("naturalEarth1"))
    st.altair_chart(final_map)


def create_heat_map_section(df):
    st.subheader(
        'Country and Genre Heat Map')

    genre_counts = df['genre'].value_counts().reset_index()

    top_genres = genre_counts['genre'][genre_counts['count'] > 50]

    cols = st.columns(2)

    with cols[0]:
        selected_genre = st.selectbox(
            "Select a Genre", [genre.title() for genre in top_genres.unique()])
    with cols[1]:
        selection = st.selectbox('Choose Type of Heat Map',
                                 ['Percentage', 'Total Count'])

    create_heat_map_graph(df,
                          selected_genre, selection)


if __name__ == "__main__":
    st.set_page_config(
        layout="wide", page_title="BandCamp Analysis", page_icon="🎵")
    load_dotenv()
    connection = get_db_connection()
    st.title('Live Analytics')

    with st.container(border=True):
        time_sample = build_date_range_slider()

    start_timestamp = pd.to_datetime(time_sample[0], utc=True)

    end_timestamp = pd.to_datetime(
        time_sample[1], utc=True) + pd.Timedelta(days=1)

    custom_css = """<style> body { background-color: #79CFE9; } </style>"""

    st.markdown(custom_css, unsafe_allow_html=True)

    with st.container(border=True):

        st.subheader(
            'Compare the Sales of Music Tracks, Artists and Genres')

        if 'button_pressed' not in st.session_state:
            st.session_state.button_pressed = False

        if 'genre_button_pressed' not in st.session_state:
            st.session_state.genre_button_pressed = False

        if 'artist_button_pressed' not in st.session_state:
            st.session_state.artist_button_pressed = False

        cols = st.columns(3)

        with cols[0]:
            if st.button("Track/Album Sales"):
                st.session_state.button_pressed = True
                st.session_state.artist_button_pressed = False
                st.session_state.genre_button_pressed = False

        with cols[1]:
            if st.button("Artist Sales"):
                st.session_state.artist_button_pressed = True
                st.session_state.button_pressed = False
                st.session_state.genre_button_pressed = False

        with cols[2]:
            if st.button("Genre Sales"):
                st.session_state.button_pressed = False
                st.session_state.artist_button_pressed = False
                st.session_state.genre_button_pressed = True

        sales = load_sales_data(connection, start_timestamp, end_timestamp)
        sales_with_genres = load_sales_genres(
            connection, start_timestamp, end_timestamp)

        if st.session_state.button_pressed:
            create_sales_chart(sales, 'item_name')

        if st.session_state.artist_button_pressed:
            create_sales_chart(sales, 'artist')

        if st.session_state.genre_button_pressed:
            create_sales_chart(sales_with_genres, 'genre')

    with st.container(border=True):
        st.subheader(
            'Analysis of Specific Tracks and Albums')

        track = st.text_input('Search for a Track or Album')

        track_data = get_specific_track_data(
            connection, start_timestamp, end_timestamp, track)

        track_data = track_data.drop_duplicates(
            subset=['artist', 'amount', 'country', 'sale_time', 'item_name'])

        cols = st.columns(2)
        with cols[0]:
            if len(track_data) > 0:
                inner_cols = st.columns(2)
                with inner_cols[0]:
                    st.markdown(
                        f'<img src="{track_data.iloc[0]["item_image"]}" style="width:100%;">',
                        unsafe_allow_html=True)
                    st.write('')
                with inner_cols[1]:

                    st.markdown(
                        """<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                        Artists:</div>""",
                        unsafe_allow_html=True)

                    st.markdown(
                        f"""<div style='padding: 4px; display: inline-block; margin: 4px; 
                                        background-color: #76D7E8; border-radius: 8px;'>
                            {track_data.iloc[0]['artist'].title()}</div>""",
                        unsafe_allow_html=True)

                    content = """<div style='padding: 4px; font-weight: bold;
                                           font-size: 20px'>Genres:</div><ul>"""

                    track_data_genres = track_data.drop_duplicates(
                        subset='genre').head(3)

                    for index, row in track_data_genres.iterrows():
                        content += f"""<li style='padding: 4px; display: inline-block; margin: 4px;
                                                  background-color: #76D7E8; border-radius: 8px;'>
                        {row['genre'].title()}</li>"""

                    content += "</ul>"

                    st.markdown(content, unsafe_allow_html=True)
                    st.write('')

            elif track.strip(' ') == '':
                pass
            else:
                st.write('Album/Track not found')

        with cols[1]:
            if len(track_data) > 0:

                most_recent_sale = track_data[track_data['sale_time']
                                              == track_data['sale_time'].max()]
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Most Recent Price: {'${:.2f}'.format(most_recent_sale.iloc[0]['amount'] / 100)}
                    </div>
                    """,
                    unsafe_allow_html=True)

                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Total Copies Sold: {len(track_data)}</div>""",
                    unsafe_allow_html=True)

                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Highest selling Price: {'${:.2f}'.format(track_data['amount'].max() /100)}
                    </div>""",
                    unsafe_allow_html=True)

        cols = st.columns(2)

        with cols[0]:
            if len(track_data) > 0:
                st.write('')
                create_price_graph(track_data)

        with cols[1]:
            if len(track_data) > 0:
                create_country_graph(track_data)

    with st.container(border=True):

        st.subheader(
            'Analysis of Specific Artists')

        artist = st.text_input('Search for an Artist')

        artist_data = get_specific_artist_data(
            connection, start_timestamp, end_timestamp, artist)
        artist_data = artist_data.drop_duplicates(
            subset=['artist', 'amount', 'country', 'sale_time', 'item_name'])

        columns = st.columns(2)
        with columns[0]:
            if len(artist_data) > 0:

                total_made = artist_data['amount'].sum()
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Total Made: {'${:.2f}'.format(total_made / 100)}</div>""",
                    unsafe_allow_html=True)

                top_countries = artist_data['country'].value_counts(
                ).reset_index()
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Top Country: {top_countries['country'].iloc[0]}</div>""",
                    unsafe_allow_html=True)

                top_genre = artist_data['genre'].value_counts(
                ).reset_index()
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Top Genre: {top_genre['genre'].iloc[0].title()}</div>""",
                    unsafe_allow_html=True)

            elif artist.strip(' ') == '':
                pass

            else:
                st.write('Artist not found')

        with columns[1]:
            if len(artist_data) > 0:
                response = get(
                    f"https://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist}&api_key={environ['API_KEY']}&format=json",
                    timeout=100)
                response = response.json()
                if 'error' not in response:
                    string = """<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                               Top 5 Similar Artists:</div><ul>"""
                    for name in response['similarartists']['artist'][:5]:
                        string += f"""<li style='padding: 4px; font-weight: normal;
                                               font-size: 16px;'>{name['name']}</li>"""
                    string += "</ul>"
                    st.markdown(string, unsafe_allow_html=True)

        if len(artist_data) > 0:
            st.write('')
            create_album_track_graph(artist_data)

    with st.container(border=True):
        genre_and_countries = load_genre_and_countries(
            connection, start_timestamp, end_timestamp)
        create_heat_map_section(genre_and_countries)
