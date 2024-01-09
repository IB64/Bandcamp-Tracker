FROM python:latest

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt 

COPY country_codes.csv .
COPY /root/pages/Newsletter.py .
COPY /root/pages/Analytics.py .
COPY Home.py .
COPY config.toml /root/.streamlit/config.toml

EXPOSE 8501

CMD streamlit run Home.py --server.port 8501
