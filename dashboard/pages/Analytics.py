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
                        'country_id', 'country', 'item_name', 'item_type',
                        'item_image', 'artist', 'genre']
        return pd.DataFrame(tuples, columns=column_names)


def build_date_range_slider() -> list[datetime]:
    """Creates a slider for user to select data sample range; default range is the last 24 hours."""
    return st.slider('Select Date Range:', min_value=pd.to_datetime(
        '2024-01-08 00:00:00+00:00', utc=True).date(),
        max_value=pd.to_datetime('now', utc=True).date(),
        value=((pd.Timestamp('now', tz='UTC') - pd.Timedelta(days=1)
                ).date(), pd.Timestamp('now', tz='UTC').date()),
        format="DD/MM/YYYY",
    )


def create_sales_chart(df, object_type):
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
    ])['amount'].count().reset_index(name='total')

    artist_chart = alt.Chart(grouped_data).mark_line().encode(
        x=alt.X('sale_time:T', title='Time'),
        y=alt.Y('total:Q', title='Number of Sales'),
        color=alt.Color(f'{object_type}:N', scale=alt.Scale(scheme='blues')),
        detail=f'{object_type}:N'
    ).properties(
        title=chart_title
    )

    st.altair_chart(artist_chart, use_container_width=True)


def create_country_graph(df):
    """Creates a bar chart showing the number of sales for each country."""
    total_sales = df.groupby('country').size().reset_index(name='count')
    top_items = total_sales.nlargest(5, 'count')
    chart = alt.Chart(top_items).mark_bar().encode(
        x=alt.X('country', title='Country'),
        y=alt.Y('count', title='Number of sales'),
        color=alt.Color('country:N', scale=alt.Scale(scheme='blues'))
    ).properties(
        title='Number of Sales in top 5 countries',
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)


def create_price_graph(df):
    """Creates a line graph showing the number of sales over time."""
    df['sale_time'] = pd.to_datetime(df['sale_time'])
    total_sales = df.groupby(
        df['sale_time'].dt.date).size().reset_index(name='count')
    chart = alt.Chart(total_sales).mark_line().encode(
        x=alt.X('sale_time:T', title='Day'),
        y=alt.Y('count', title='Number of Copies Sold'),
    ).properties(
        title='Number of Sales in the last 5 days',
        width=400,
        height=300
    )
    st.altair_chart(chart, use_container_width=True)


def create_album_track_graph(df):
    """Creates a bar chart showing the number of sales for each album/track an artist has."""
    total_sales = df.groupby('item_name').size().reset_index(name='count')
    top_items = total_sales.nlargest(5, 'count')

    chart = alt.Chart(top_items).mark_bar().encode(
        x=alt.X('item_name', title='Item'),
        y=alt.Y('count', title='Number of Copies sold'),
        color=alt.Color('item_name:N', scale=alt.Scale(scheme='blues'))
    ).properties(
        title='Number of Sales for the artists top albums/tracks',
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)


def create_heat_map_graph(df):
    """Creates a country-genre heat map, showing where genres are most popular in the world."""
    country_count_df = df['country'].value_counts(
    ).reset_index(name='popularity')

    country_count_df['country'] = country_count_df['country'].replace(
        'United Kingdom', 'United Kingdom of Great Britain and Northern Ireland')
    country_count_df['country'] = country_count_df['country'].replace(
        'United States', 'United States of America')

    source = alt.topo_feature(data.world_110m.url, "countries")

    country_codes = pd.read_csv("country_codes.csv")

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
        from_=alt.LookupData(country_count_df, 'country', [
            'popularity'])
    )

    final_map = ((background + foreground).configure_view(strokeWidth=0).properties(
        width=500).project("naturalEarth1"))
    st.altair_chart(final_map)


if __name__ == "__main__":
    load_dotenv()
    connection = get_db_connection()
    duplicate_df = load_all_data(connection)

    st.set_page_config(
        layout="wide", page_title="BandCamp Analysis", page_icon="🎵")

    custom_css = """<style> body { background-color: #79CFE9; } </style>"""

    st.markdown(custom_css, unsafe_allow_html=True)

    st.title('Live Analytics')

    non_duplicate_df = duplicate_df.drop_duplicates(
        subset=['sale_time', 'amount', 'item_name', 'artist'])

    with st.container(border=True):
        time_sample = build_date_range_slider()

    start_timestamp = pd.to_datetime(time_sample[0], utc=True)
    end_timestamp = pd.to_datetime(
        time_sample[1], utc=True) + pd.Timedelta(days=1)

    filtered_df = non_duplicate_df[(non_duplicate_df['sale_time'] >= start_timestamp) & (
        non_duplicate_df['sale_time'] <= end_timestamp)]

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
            create_sales_chart(filtered_df, 'item_name')

        if st.session_state.artist_button_pressed:
            create_sales_chart(filtered_df, 'artist')

        filtered_duplicate_data = duplicate_df[(duplicate_df['sale_time'] >= start_timestamp) & (
            duplicate_df['sale_time'] <= end_timestamp)]

        if st.session_state.genre_button_pressed:
            create_sales_chart(filtered_duplicate_data, 'genre')

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
                        f'<img src="{filtered_data.iloc[0]["item_image"]}" style="width:100%;">',
                        unsafe_allow_html=True)
                with inner_cols[1]:

                    filtered_track_data = duplicate_df[duplicate_df['item_name'] == track]

                    filtered_track_data = filtered_track_data.drop_duplicates(
                        subset=['genre', 'artist']).head(3)

                    st.markdown(
                        """<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                        Artists:</div>""",
                        unsafe_allow_html=True)

                    st.markdown(
                        f"""<div style='padding: 4px; display: inline-block; margin: 4px; 
                                        background-color: #76D7E8; border-radius: 8px;'>
                            {filtered_track_data.iloc[0]['artist'].title()}</div>""",
                        unsafe_allow_html=True)

                    content = """<div style='padding: 4px; font-weight: bold;
                                           font-size: 20px'>Genres:</div><ul>"""

                    for index, row in filtered_track_data.iterrows():
                        content += f"""<li style='padding: 4px; display: inline-block; margin: 4px;
                                                  background-color: #76D7E8; border-radius: 8px;'>
                        {row['genre'].title()}</li>"""

                    content += "</ul>"

                    st.markdown(content, unsafe_allow_html=True)

                create_price_graph(filtered_data)

        with cols[1]:
            if len(filtered_data) > 0:
                create_country_graph(filtered_data)

                filtered_all_data = non_duplicate_df[non_duplicate_df['item_name'] == track]
                most_recent_sale = filtered_data[filtered_data['sale_time']
                                                 == filtered_data['sale_time'].max()]

                st.write("")

                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Most Recent Price: {'${:.2f}'.format(most_recent_sale.iloc[0]['amount'] / 100)}
                    </div>""",
                    unsafe_allow_html=True)

                filtered_track_data = non_duplicate_df[non_duplicate_df['item_name'] == track]
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Total Copies Sold: {len(filtered_track_data)}</div>""",
                    unsafe_allow_html=True)

                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Highest selling Price: {'${:.2f}'.format(filtered_all_data['amount'].max() /100)}
                    </div>""",
                    unsafe_allow_html=True)

    with st.container(border=True):

        st.subheader(
            'Analysis of Specific Artists')
        columns = st.columns(2)
        with columns[0]:
            artist = st.text_input('Search for an Artist')
            filtered_artist = filtered_df[filtered_df['artist'] == artist]
            if len(filtered_artist) > 0:
                filtered_artist = filtered_artist.drop_duplicates(
                    subset=['sale_time', 'amount', 'genre', 'item_name', 'artist'])

                total_made = filtered_artist['amount'].sum()
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Total Made: {'${:.2f}'.format(total_made / 100)}</div>""",
                    unsafe_allow_html=True)

                top_countries = filtered_artist['country'].value_counts(
                ).reset_index()
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Top Country: {top_countries['country'].iloc[0]}</div>""",
                    unsafe_allow_html=True)

                top_genre = filtered_artist['genre'].value_counts(
                ).reset_index()
                st.markdown(
                    f"""<div style='padding: 4px; font-weight: bold; font-size: 20px'>
                    Top Genre: {top_genre['genre'].iloc[0].title()}</div>""",
                    unsafe_allow_html=True)

        with columns[1]:
            if len(filtered_artist) > 0:
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

        if len(filtered_artist) > 0:
            create_album_track_graph(filtered_artist)

    with st.container(border=True):
        st.subheader(
            'Country/Genre heat map')

        selected_genre = st.selectbox(
            "Select a Genre", filtered_duplicate_data['genre'].unique())

        genre_filtered_data = filtered_duplicate_data[filtered_duplicate_data['genre']
                                                      == selected_genre]
        create_heat_map_graph(genre_filtered_data)
