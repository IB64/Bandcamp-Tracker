"""Newsletter page for the StreamLit dashboard, where users can subscribe to the daily email."""
from os import environ
import re
from dotenv import load_dotenv
from psycopg2 import extensions, connect

from boto3 import client
import streamlit as st


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


def send_confirmation_email(email):
    """Given an email, adds it to SES on AWS."""
    ses = client('ses',
                 aws_access_key_id=environ["AWS_ACCESS_KEY_ID"],
                 aws_secret_access_key=environ["AWS_SECRET_ACCESS_KEY"])

    ses.verify_email_identity(
        EmailAddress=email
    )


def add_subscriber(connection, user_email):
    """Adds the email address to the subscriber table in the database."""
    with connection.cursor() as cur:
        cur.execute(
            f"INSERT INTO subscribers(subscriber_email) VALUES ('{user_email}') ON CONFLICT DO NOTHING;")
        connection.commit()


def main(connection):
    """Main function to create the Newsletter page description."""
    st.write("# Newsletter Email")

    with st.container(border=True):
        st.markdown(
            """
        Our Daily Music Insights Newsletter delivers a curated experience straight to your inbox, 
        providing you with a recap of the previous day's music scene, 
        exclusive insights into top artists, genres, and albums, 
        and a glimpse into regional music trends.
        ## Why Subscribe?
        - **Daily Highlights:**
            Receive a daily dose of music by staying up to date with sales from the previous day.

        - **Detailed Top Chart Data:**
            Discover who's taking the music scene by storm. 
            Our newsletter showcases insights into the top-performing artists and trending genres, 
            helping you explore new sounds and talents.

        - **Regional Data:**
            Explore how music trends vary across the globe and broaden your musical horizons.

        **To start receiving your daily music insights,** 
        simply enter your email address to subscribe below:
        """)

        st.text_input('Enter email address: ', key='user_email')

        EMAIL = r'^([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$'

        if st.button('Subscribe'):
            user_email = st.session_state.user_email
            if user_email == '':
                pass
            elif not re.match(EMAIL, user_email):
                st.write('Not a valid Email')
            else:
                send_confirmation_email(user_email)
                add_subscriber(connection, user_email)
                st.markdown(" Email has been subscribed! ")
                st.markdown(
                    "Please confirm your email address in the most recent email from AWS.")


if __name__ == "__main__":
    load_dotenv()
    st.set_page_config(
        page_title="BandCamp Analytics",
        page_icon="🎵",)
    conn = get_db_connection()
    main(conn)
