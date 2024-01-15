FROM public.ecr.aws/lambda/python:latest

WORKDIR ${LAMBDA_TASK_ROOT}
RUN mkdir static

RUN dnf -y install cairo-devel gcc

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY static/style.css static
COPY bandcamp_logo.jpeg .
COPY report.py .

CMD [ "report.handler" ]