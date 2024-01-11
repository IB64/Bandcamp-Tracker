# Dashboard

This folder contains the files to create the live StreamLit dashboard. The requirements to run the dashboard are below.

## Requirements

To add the necessary requirements to run the dashboard:

- Run `pip install -r requirements.txt`

Add these variables to a .env file:

- DB_USER
- DB_NAME
- DB_PASSWORD
- DB_PORT
- DB_IP
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- TOPIC_ARN
- API_KEY

## Running

To run the streamlit dashboard:

- Run `streamlit run Home.py`

## Dashboard Pages

The dashboard is made of several pages. A brief description of each one is below:

### Home.py

This creates the Home page for the dashboard. When the dashboard is run, this is the default page that shows up.

### Analytics.py

This page creates the live dashboard analytics page, showing graphs about albums, artists and genres. The time range can be changed and the graphs update as the database updates.

### Newsletter.py

This page contains information about the daily Newsletter email. A user can subscribe to the newsletter on this page.