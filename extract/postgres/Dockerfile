FROM python:3.7

WORKDIR /postgres/tap_postgres

COPY requirements.txt ./
RUN pip install -U pip
RUN pip install -r requirements.txt

COPY . ./

