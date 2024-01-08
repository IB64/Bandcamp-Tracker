"""Newsletter page for the StreamLit dashboard, where users can subscribe and unsubscribe from the daily email."""
from os import environ
import re
from dotenv import load_dotenv

import streamlit as st
from boto3 import client


def subscribe_email_to_sns(email):
    """Given an email, subscribes it to the SNS topic on AWS."""
    sns_client = client(
        'sns', aws_access_key_id=environ["AWS_ACCESS_KEY_ID"], aws_secret_access_key=environ["AWS_SECRET_ACCESS_KEY"])

    sns_client.subscribe(
        TopicArn=environ["TOPIC_ARN"],
        Protocol='Email',
        Endpoint=email
    )


def main():
    st.set_page_config(
        layout="wide", page_title="BandCamp Analysis", page_icon="🎵")

    st.write("# Newsletter Email")

    with st.container(border=True):

        st.markdown("<div style='padding: 4px; font-size: 18px'>Our Daily Music Insights Newsletter delivers a curated experience straight to your inbox, providing you with a recap of the previous day's music scene, exclusive insights into top artists, genres, and albums, and a glimpse into regional music trends.</div>", unsafe_allow_html=True)

        st.markdown("<div style='padding: 4px; font-weight: bold; font-size: 30px'>Why Subscribe?</div>",
                    unsafe_allow_html=True)

        st.markdown("""<ul style='padding: 4px;'>
                    <li style='font-size: 20px; font-weight: bold;'>Daily Highlights
                        <div style='font-size: 16px; font-weight: lighter;'>Receive a daily dose of music by staying up to date with sales from the previous day.</div>
                    </li>
                    <li style='font-size: 20px; font-weight: bold;'>Detailed Top Chart data 
                    <div style='font-size: 16px; font-weight: lighter;'>Discover who's taking the music scene by storm. Our newsletter showcases insights into the top-performing artists and trending genres, helping you explore new sounds and talents.</div>
                    </li>
                    <li style='font-size: 20px; font-weight: bold;'>Regional Data
                    <div style='font-size: 16px; font-weight: lighter;'>Explore how music trends vary across the globe and broaden your musical horizons.</div>
                    </li>
                        </ul>""",
                    unsafe_allow_html=True)

        st.markdown("<div style='padding: 4px; font-size: 18px'>To start receiving your daily music insights, simply enter your email address to subscribe below:</div>", unsafe_allow_html=True)
        st.write(" ")
        user_email = st.text_input('Enter email address: ')

        EMAIL = r'^([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$'

        if user_email == '':
            pass
        elif not re.match(EMAIL, user_email):
            st.write('Not a valid Email')
        else:
            subscribe_email_to_sns(user_email)
            st.markdown(
                "<div style='padding: 4px; font-size: 16px'>Email has been subscribed! Please confirm your email address by clicking 'Confirm Subscription' in your most recent email from AWS Notifications.</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    load_dotenv()
    main()
