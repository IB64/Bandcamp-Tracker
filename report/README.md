# Bandcamp-Tracker Report

This folder contains all the necessary files to produce the Bandcamp Daily report.

The Bandcamp Daily Report offers a detailed exploration of sales data, providing valuable insights into the music industry's dynamics. By analysing data from the previous day the report aims to offer a snapshot of trends and patterns in music purchases as well as trending genres and regional data.

The Report contains:
- Overview
- Contents
- Key Metrics
- Top Performers
- Sales Overview
- Genre Analysis
- Regional Insights

## Files explained

This folder is divided into multiple files/folders where each file tackles a different part of generating the report.

- `requirements.txt` - This file contains all the required python modules that are needed to run `report.py`.

- `report.py` - This files contains the code for generating the report and sending an email with the report as an attachment. The report is generated by querying the bandcamp database using SQL and then using pandas to further query. The result are then put into a html string which is then converted to a PDF report. This file is used to make a AWS lambda and therefore contains a handler function which runs all the necessary functions.

- `Dockerfile` - This file contains the code that creates a docker image with it's base image as a AWS lambda. Once the docker image has been made, you can tag the image to an AWS ECR.

- `bandcamp_logo.jpeg` - This file contains an image of the bandcamp logo which is used when creating the report

- `static` - This is a folder that contains the `style.css` file which has contains all the css for the report.

## Set Up

These are the set up instructions and all the necessary requirements to run the files in this folder:

1. Set up a `.env` file and add the following variables:
    - DB_USER
    - DB_NAME
    - DB_PASSWORD
    - DB_PORT
    - DB_IP
    - AWS_ACCESS_KEY_ID_
    - AWS_SECRET_ACCESS_KEY_

2. Set up a venv (virtual environment). You can do this by running the following commands:
    - `python3 -m venv venv` : creates the venv
    - `source ./venv/bin/activate` : activates the venv

3. Install all the required modules in `requirements.txt` by running:
    - `pip3 install -r requirements.txt`


## Running the Files

To run the `report.py` use the following command:
- `python3 report.py`
After running this command, you should see that a PDF called `Bandcamp-Daily-Report.pdf` has been made and added to this folder.

To make your docker image use the following command:
- `docker build -t "name_of_image" --platform "linux/amd64"` - This will build your docker image so that it can be used on AWS and is built for linux machines.


## ECR and Docker Image

Once you have successfully made your docker image using the command in the section above, you can tag your image to an AWS ECR. To do this, you will need to ensure that you have created an AWS ECR. When you have created your AWS ECR, you can use the following commands to tag your docker image to it.
 - `aws ecr get-login-password --region 'your-region' | docker login --username AWS --password-stdin 'your_aws_account_id'.dkr.ecr.region.amazonaws.com`
 - `docker images` : This will allow you to find your image id
 - `docker tag 'your docker image id' 'your ECR URI'`
 - `docker push 'your ECR URI'`