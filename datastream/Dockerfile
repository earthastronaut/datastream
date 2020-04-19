FROM python:3.8-slim-buster

RUN apt-get update -y
RUN apt-get install -y \
	libpq-dev \
	gcc
# libpq-dev \ postgres libraries
# gcc \ help installing psycopg 

WORKDIR /usr/local/src/

ENV PYTHONPATH '/usr/local/src/'
RUN pip install --no-cache-dir psycopg2

COPY . .

CMD ["python"]
