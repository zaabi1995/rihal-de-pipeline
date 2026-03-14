FROM apache/airflow:2.7.3-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==1.4.48 \
    pytest==7.4.4 \
    pytest-cov==4.1.0

# Set working directory
WORKDIR /opt/airflow
