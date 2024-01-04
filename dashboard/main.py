import streamlit as st
import altair as alt
import pandas as pd
from datetime import datetime

from psycopg2 import extensions, connect

from os import environ

from dotenv import load_dotenv


st.set_page_config(
    layout="wide", page_title="BandCamp Analysis", page_icon="🎵"
)

custom_css = """<style>
                        body {
                            background-color: #79CFE9;
                        }
                    </style>
                """

st.markdown(custom_css, unsafe_allow_html=True)


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
                    SELECT sale_event.*, country.country, item.item_name, item.item_type_id, artist.artist_name, genre.genre
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
                        'country_id', 'country', 'item_name', 'item_type', 'artist', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


def build_date_range_slider() -> list[datetime]:
    """
    Builds slider for user to select data sample range; default selected range is only the current
    day.
    """
    return st.slider('Select Time Range: ',
                     min_value=pd.to_datetime(
                         '2024-01-01 00:00:00+00:00', utc=True).date(),
                     max_value=pd.to_datetime('now', utc=True).date(),
                     value=((pd.Timestamp('now', tz='UTC') - pd.Timedelta(days=1)
                             ).date(), pd.Timestamp('now', tz='UTC').date()))


def create_sales_track_chart(data):

    track_suggestions = set([
        f"{track} (Track)" if item_type == 1 else f"{track} (Album)"
        for track, item_type in zip(data['item_name'], data['item_type'])
    ])

    selected_tracks = st.multiselect(
        "Select Track/Albums", track_suggestions)

    selected_tracks_info = [track.split(" (")[0] for track in selected_tracks]

    if selected_tracks:
        filtered_data = data[data['item_name'].isin(selected_tracks_info)]
        chart_title = 'Sales Over Time -'
        for track in selected_tracks:
            if len(chart_title) == 17:
                chart_title += f' {track}'
            else:
                chart_title += f', {track}'
    else:
        chart_title = 'Sales Over Time - All Tracks'
        filtered_data = data

    if len(filtered_data) == 0:
        chart_title = 'Sales Over Time - All Tracks'
        filtered_data = data

    grouped_data = filtered_data.groupby(['sale_time', 'item_name'])[
        'amount'].sum().reset_index(name='total')

    chart = alt.Chart(grouped_data).mark_line().encode(
        x='sale_time:T',
        y='total:Q',
        color=alt.Color('item_name:N', scale=alt.Scale(scheme='blues')),
        detail='item_name:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(chart, use_container_width=True)


def create_sales_artist_chart(data):

    artist_suggestions = set([
        artist for artist in data['artist']])

    selected_artists = st.multiselect(
        "Select Artists", artist_suggestions)

    if selected_artists:
        filtered_data = data[data['artist'].isin(selected_artists)]
        chart_title = 'Sales Over Time -'
        for artist in selected_artists:
            if len(chart_title) == 17:
                chart_title += f' {artist}'
            else:
                chart_title += f', {artist}'
    else:
        chart_title = 'Sales Over Time - All Artist'
        filtered_data = data

    if len(filtered_data) == 0:
        chart_title = 'Sales Over Time - All Artists'
        filtered_data = data

    grouped_data = filtered_data.groupby(['sale_time', 'artist'])[
        'amount'].sum().reset_index(name='total')

    artist_chart = alt.Chart(grouped_data).mark_line().encode(
        x='sale_time:T',
        y='total:Q',
        color=alt.Color('artist:N', scale=alt.Scale(scheme='blues')),
        detail='artist:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(artist_chart, use_container_width=True)


def create_sales_genre_chart(data):

    genre_suggestions = set([
        genre for genre in data['genre']])

    selected_genres = st.multiselect(
        "Select Genres", genre_suggestions)

    if selected_genres:
        filtered_data = data[data['genre'].isin(selected_genres)]
        chart_title = 'Sales Over Time -'
        for genre in selected_genres:
            if len(chart_title) == 17:
                chart_title += f' {genre}'
            else:
                chart_title += f', {genre}'
    else:
        chart_title = 'Sales Over Time - All Genres'
        filtered_data = data

    if len(filtered_data) == 0:
        chart_title = 'Sales Over Time - All Genres'
        filtered_data = data

    grouped_data = filtered_data.groupby(['sale_time', 'genre'])[
        'amount'].sum().reset_index(name='total')

    genre_chart = alt.Chart(grouped_data).mark_line().encode(
        x='sale_time:T',
        y='total:Q',
        color=alt.Color('genre:N', scale=alt.Scale(scheme='blues')),
        detail='genre:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(genre_chart, use_container_width=True)


if __name__ == "__main__":
    load_dotenv()
    connection = get_db_connection()
    duplicate_df = load_all_data(connection)

    st.title('BandCamp Analysis 🎵')

    with st.container(border=True):
        st.subheader('Top Charts')

        cols = st.columns(2)
        with cols[0]:
            st.write('Top Artists')

        with cols[1]:
            st.write('Highest earning albums')

    df = duplicate_df.drop_duplicates(
        subset=['sale_time', 'amount', 'item_name'])

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
            if st.button("Compare Track and Album Sales"):
                st.session_state.button_pressed = True
                st.session_state.artist_button_pressed = False
                st.session_state.genre_button_pressed = False

        with cols[1]:
            if st.button("Compare Artist Sales"):
                st.session_state.artist_button_pressed = True
                st.session_state.button_pressed = False
                st.session_state.genre_button_pressed = False

        with cols[2]:
            if st.button("Compare Genre Sales"):
                st.session_state.button_pressed = False
                st.session_state.artist_button_pressed = False
                st.session_state.genre_button_pressed = True

        time_sample = build_date_range_slider()
        start_date_timestamp = pd.to_datetime(time_sample[0], utc=True)
        end_date_timestamp = pd.to_datetime(
            time_sample[1], utc=True) + pd.Timedelta(days=1)

        filtered_df = df[(df['sale_time'] >= start_date_timestamp) & (
            df['sale_time'] <= end_date_timestamp)]

        if st.session_state.button_pressed:
            create_sales_track_chart(filtered_df)

        if st.session_state.artist_button_pressed:
            create_sales_artist_chart(filtered_df)

        filtered_duplicate_data = duplicate_df[(duplicate_df['sale_time'] >= start_date_timestamp) & (
            duplicate_df['sale_time'] <= end_date_timestamp)]

        if st.session_state.genre_button_pressed:
            create_sales_genre_chart(filtered_duplicate_data)

    with st.container(border=True):
        st.subheader(
            'Analysis of Specific Tracks and Albums')

        st.text_input('Search for a Track or Album')
