FROM python:latest

COPY requirements.txt .

RUN pip3 install -r requirements.txt

RUN python -m spacy download en_core_web_sm

COPY extract.py .

COPY transform.py .

COPY load.py .

COPY pipeline.py .

CMD python3 pipeline.py