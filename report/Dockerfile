FROM public.ecr.aws/lambda/python:latest

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY static .
COPY bandcamp_logo.jpeg .
COPY report.py .

CMD [ "report.handler" ]