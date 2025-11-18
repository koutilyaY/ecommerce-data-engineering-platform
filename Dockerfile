# Start from the official Airflow image
FROM apache/airflow:2.9.1-python3.11

# Copy requirements into the container
COPY requirements.txt /requirements.txt

# Install the Python packages needed for Kafka and Postgres
RUN pip install --no-cache-dir -r /requirements.txt