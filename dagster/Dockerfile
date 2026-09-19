# Dockerfile
FROM python:3.11-slim

# Install system dependencies needed for libraries like psycopg2
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Dagster open source orchestration engine layers
RUN pip install \
    dagster \
    dagster-postgres \
    dagster-webserver \
    dagster-docker

WORKDIR /opt/dagster/app

# Copy your local pipelines code definitions to the target folder
COPY . /opt/dagster/app

# Expose port for gRPC discovery
EXPOSE 4000

# Start code server endpoint
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "my_dagster_project"]

