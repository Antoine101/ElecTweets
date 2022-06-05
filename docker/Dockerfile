FROM python:3

WORKDIR /app

COPY docker/requirements.txt requirements.txt
COPY . .

RUN pip install --no-cache-dir -r requirements.txt