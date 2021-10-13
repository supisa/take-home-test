FROM python:3.9 as etl

WORKDIR /opt
RUN apt-get update
RUN apt-get install default-jdk -y

RUN pip install "poetry==1.1.7"
COPY poetry.lock pyproject.toml /opt/
RUN poetry config virtualenvs.create false && poetry install  --no-interaction --no-ansi
COPY main.py .
COPY db_properties.ini .