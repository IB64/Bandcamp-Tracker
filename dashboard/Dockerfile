FROM python:latest

WORKDIR /app
RUN mkdir pages
RUN mkdir .streamlit

COPY requirements.txt .
RUN pip3 install -r requirements.txt 

COPY country_codes.csv .
COPY /pages/Newsletter.py pages
COPY /pages/Dashboard.py pages
COPY Home.py .
COPY /.streamlit/config.toml .streamlit

EXPOSE 8501

CMD streamlit run Home.py --server.port 8501