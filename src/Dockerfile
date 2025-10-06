FROM python:3.13-alpine3.20

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apk add --no-cache \
    freetds-dev \
    unixodbc-dev \
    gcc \
    g++ \
    musl-dev \
    linux-headers \
    postgresql-dev \
    lapack-dev \
    gfortran \
    openblas-dev

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs

COPY . .

CMD ["python", "proyecto_mineria.py"]
