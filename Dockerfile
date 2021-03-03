FROM python:3.8-slim-buster

COPY ./src /src
COPY requirements.txt .

RUN apt-get update
RUN apt-get install -y gcc
RUN rm -rf /var/lib/apt/lists/

RUN python3 -m pip install -r requirements.txt