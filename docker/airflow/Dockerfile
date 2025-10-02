FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y postgresql-client && apt-get clean

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

# Copy password file template to Airflow working directory
COPY --chown=airflow:root simple_auth_manager_passwords.json.generated /opt/airflow/simple_auth_manager_passwords.json.generated