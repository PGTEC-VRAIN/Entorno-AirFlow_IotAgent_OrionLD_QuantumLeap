FROM apache/airflow:3.0.6

USER root
# Opcional: instalar dependencias de sistema si lo necesitas
# RUN apt-get update && apt-get install -y vim less && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt