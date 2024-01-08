"""Newsletter page for the StreamLit dashboard, where users can subscribe to the daily email."""
from os import environ
import re
from dotenv import load_dotenv

from boto3 import client
import streamlit as st


def subscribe_email_to_sns(email):
    """Given an email, subscribes it to the SNS topic on AWS."""
    sns_client = client('sns',
                        aws_access_key_id=environ["AWS_ACCESS_KEY_ID"],
                        aws_secret_access_key=environ["AWS_SECRET_ACCESS_KEY"])

    sns_client.subscribe(
        TopicArn=environ["TOPIC_ARN"],
        Protocol='Email',
        Endpoint=email
    )


def main():
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
                subscribe_email_to_sns(user_email)
                st.markdown(
                    """
                    **Email has been subscribed!**
                    Please confirm your email address by clicking 'Confirm Subscription' 
                    in your most recent email from AWS Notifications.
                    """
                )


if __name__ == "__main__":
    load_dotenv()
    main()
