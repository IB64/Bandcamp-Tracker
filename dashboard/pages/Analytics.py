import streamlit as st
import altair as alt
import pandas as pd
from datetime import datetime
from vega_datasets import data

from psycopg2 import extensions, connect

from os import environ

from dotenv import load_dotenv


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


def build_date_range_slider() -> list[datetime]:
    """
    Builds slider for user to select data sample range; default selected range is only the current
    day.
    """
    return st.slider('Select Date Range:', min_value=pd.to_datetime(
        '2024-01-04 00:00:00+00:00', utc=True).date(),
        max_value=pd.to_datetime('now', utc=True).date(),
        value=((pd.Timestamp('now', tz='UTC') - pd.Timedelta(days=1)
                ).date(), pd.Timestamp('now', tz='UTC').date()),
        format="DD/MM/YYYY",
    )


def create_sales_track_chart(data):

    track_suggestions = set([
        f"{track} (Track)" if item_type == 1 else f"{track} (Album)"
        for track, item_type in zip(data['item_name'], data['item_type'])
    ])

    selected_tracks = st.multiselect(
        "Select Track/Albums", track_suggestions)

    selected_tracks_info = [track.split(" (")[0] for track in selected_tracks]

    chart_title = 'Sales Over Time - Top Tracks'

    if selected_tracks:
        selected_df = data[data['item_name'].isin(selected_tracks_info)]
        chart_title = 'Sales Over Time -'
        for item in selected_tracks:
            if len(chart_title) == 17:
                chart_title += f' {item}'
            else:
                chart_title += f', {item}'
    else:
        top_tracks = df['item_name'].value_counts().head(10).index.tolist()
        selected_df = df[df['item_name'].isin(top_tracks)]

    grouped_data = selected_df.groupby(['sale_time', 'item_name'])[
        'amount'].sum().reset_index(name='total')
    grouped_data = grouped_data.sort_values(by='sale_time')
    grouped_data['total'] = grouped_data['total'] / 100

    chart = alt.Chart(grouped_data).mark_line().encode(
        x=alt.X('sale_time:T', title='Sale Time'),
        y=alt.Y('total:Q', title='Total Amount'),
        color=alt.Color('item_name:N', scale=alt.Scale(scheme='blues')),
        detail='item_name:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(chart, use_container_width=True)


def create_sales_artist_chart(df):

    artist_suggestions = set([
        artist for artist in df['artist']])

    selected_artists = st.multiselect(
        "Select Artists", artist_suggestions)

    chart_title = 'Sales Over Time - Top Artists'

    if selected_artists:
        selected_df = df[df['artist'].isin(selected_artists)]
        chart_title = 'Sales Over Time -'
        for artist_name in selected_artists:
            if len(chart_title) == 17:
                chart_title += f' {artist_name}'
            else:
                chart_title += f', {artist_name}'
    else:
        top_artists = df['artist'].value_counts().head(10).index.tolist()
        selected_df = df[df['artist'].isin(top_artists)]

    grouped_data = selected_df.groupby(['sale_time', 'artist'])[
        'amount'].sum().reset_index(name='total')
    grouped_data['total'] = grouped_data['total'] / 100

    artist_chart = alt.Chart(grouped_data).mark_line().encode(
        x='sale_time:T',
        y='total:Q',
        color=alt.Color('artist:N', scale=alt.Scale(scheme='blues')),
        detail='artist:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(artist_chart, use_container_width=True)


def create_sales_genre_chart(df):

    genre_suggestions = set([
        genre for genre in df['genre']])

    selected_genres = st.multiselect(
        "Select Genres", genre_suggestions)

    chart_title = 'Sales Over Time - Top Genres'

    if selected_genres:
        selected_df = df[df['genre'].isin(selected_genres)]
        chart_title = 'Sales Over Time -'
        for genre in selected_genres:
            if len(chart_title) == 17:
                chart_title += f' {genre}'
            else:
                chart_title += f', {genre}'
    else:
        top_genres = df['genre'].value_counts().head(10).index.tolist()
        selected_df = df[df['genre'].isin(top_genres)]

    grouped_data = selected_df.groupby(['sale_time', 'genre'])[
        'amount'].sum().reset_index(name='total')
    grouped_data['total'] = grouped_data['total'] / 100

    genre_chart = alt.Chart(grouped_data).mark_line().encode(
        x='sale_time:T',
        y='total:Q',
        color=alt.Color('genre:N', scale=alt.Scale(scheme='blues')),
        detail='genre:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(genre_chart, use_container_width=True)


def create_country_graph(data):

    total_sales = data.groupby('country').size().reset_index(name='count')

    chart = alt.Chart(total_sales).mark_bar().encode(
        x='country',
        y='count',
        color=alt.Color('country:N', scale=alt.Scale(scheme='blues'))
    ).properties(
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)


def create_price_graph(df):

    df['sale_time'] = pd.to_datetime(df['sale_time'])

    total_sales = df.groupby(
        df['sale_time'].dt.date).size().reset_index(name='count')

    chart = alt.Chart(total_sales).mark_line().encode(
        x='sale_time:T',
        y='count'
    ).properties(
        title='Number of Sales Over Time',
        width=400,
        height=300
    )
    st.altair_chart(chart, use_container_width=True)


if __name__ == "__main__":
    load_dotenv()
    connection = get_db_connection()
    duplicate_df = load_all_data(connection)

    print(duplicate_df)

    st.set_page_config(
        layout="wide", page_title="BandCamp Analysis", page_icon="🎵")

    custom_css = """<style> body { background-color: #79CFE9; } </style>"""

    st.markdown(custom_css, unsafe_allow_html=True)

    st.title('Live Analytics')

    df = duplicate_df.drop_duplicates(
        subset=['sale_time', 'amount', 'item_name', 'artist'])

    with st.container(border=True):
        time_sample = build_date_range_slider()

    start_date_timestamp = pd.to_datetime(time_sample[0], utc=True)
    end_date_timestamp = pd.to_datetime(
        time_sample[1], utc=True) + pd.Timedelta(days=1)

    filtered_df = df[(df['sale_time'] >= start_date_timestamp) & (
        df['sale_time'] <= end_date_timestamp)]

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

        cols = st.columns(2)

        with cols[0]:
            track = st.text_input('Search for a Track or Album')
            filtered_data = filtered_df[filtered_df['item_name'] == track]

            if len(filtered_data) > 0:
                inner_cols = st.columns(2)
                with inner_cols[0]:
                    st.markdown(
                        f'<img src="{filtered_data.iloc[0]["item_image"]}" style="width:100%;">', unsafe_allow_html=True)
                with inner_cols[1]:

                    filtered_track_data = duplicate_df[duplicate_df['item_name'] == track]

                    filtered_track_data = filtered_track_data.drop_duplicates(
                        subset=['genre', 'artist']).head(3)

                    st.markdown(
                        "<div style='padding: 4px; font-weight: bold; font-size: 20px'>Artists:</div>", unsafe_allow_html=True)

                    st.markdown(
                        f"<div style='padding: 4px; display: inline-block; margin: 4px; background-color: #76D7E8; border-radius: 8px;'>{filtered_track_data.iloc[0]['artist'].title()}</div>", unsafe_allow_html=True)

                    content = "<div style='padding: 4px; font-weight: bold; font-size: 20px'>Genres:</div><ul>"

                    for index, row in filtered_track_data.iterrows():
                        content += f"<li style='padding: 4px; display: inline-block; margin: 4px; background-color: #76D7E8; border-radius: 8px;'>{row['genre'].title()}</li>"

                    content += "</ul>"

                    st.markdown(content, unsafe_allow_html=True)

                create_price_graph(filtered_data)

        with cols[1]:
            if len(filtered_data) > 0:
                create_country_graph(filtered_data)

                filtered_all_data = df[df['item_name'] == track]
                most_recent_sale = filtered_data[filtered_data['sale_time']
                                                 == filtered_data['sale_time'].max()]

                st.write("")
                st.write(
                    f"Most Recent Price: {'${:.2f}'.format(most_recent_sale.iloc[0]['amount'] / 100)}")

                filtered_track_data = df[df['item_name'] == track]
                st.write(f'Total Copies Sold: {len(filtered_track_data)}')

    with st.container(border=True):

        st.subheader(
            'Analysis of Specific Artists')
        columns = st.columns(2)
        with columns[0]:
            artist = st.text_input('Search for an Artist')
            filtered_artist = filtered_df[filtered_df['artist'] == artist]

            if len(filtered_artist) > 0:
                st.write('artist exists')

    with st.container(border=True):
        st.subheader(
            'Country/Genre heat map')

        selected_genre = st.selectbox(
            "Select a Genre", filtered_duplicate_data['genre'].unique())

        genre_filtered_data = filtered_duplicate_data[filtered_duplicate_data['genre']
                                                      == selected_genre]

        grouped_genre_data = genre_filtered_data['country'].value_counts(
        ).reset_index(name='popularity')

        grouped_genre_data['country'] = grouped_genre_data['country'].replace(
            'United Kingdom', 'United Kingdom of Great Britain and Northern Ireland')
        grouped_genre_data['country'] = grouped_genre_data['country'].replace(
            'United States', 'United States of America')

        source = alt.topo_feature(data.world_110m.url, "countries")

        country_codes = pd.read_csv(
            "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
        )

        background = alt.Chart(source).mark_geoshape(fill="white")

        foreground = alt.Chart(source).mark_geoshape(
            stroke="black", strokeWidth=0.15
        ).encode(
            color=alt.Color(
                "popularity:N", scale=alt.Scale(scheme="blues"), legend=None,
            ),
            tooltip=[
                alt.Tooltip("name:N", title="Country"),
                alt.Tooltip("popularity:Q", title="Genre Count"),
            ],
        ).transform_lookup(
            lookup="id",
            from_=alt.LookupData(data=country_codes,
                                 key="country-code", fields=["name"]),
        ).transform_lookup(
            lookup='name',
            from_=alt.LookupData(grouped_genre_data, 'country', [
                                 'popularity'])
        )

        final_map = ((background + foreground).configure_view(strokeWidth=0).properties(width=500).project("naturalEarth1")
                     )

        st.altair_chart(final_map)
