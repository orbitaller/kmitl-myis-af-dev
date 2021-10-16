FROM apache/airflow:2.2.0

RUN pip install --no-cache-dir 'apache-airflow-providers-oracle'
