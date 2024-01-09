FROM public.ecr.aws/lambda/python:latest

WORKDIR ${LAMBDA_TASK_ROOT}

RUN dnf -y install cairo-devel gcc

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY static .
COPY bandcamp_logo.jpeg .
COPY report.py .

CMD [ "report.handler" ]