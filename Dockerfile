FROM apache/airflow:2.10.4
USER airflow
RUN pip install --no-cache-dir openpyxl
RUN pip install --no-cache-dir openpyxl pandas numpy