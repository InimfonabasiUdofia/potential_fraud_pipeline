
FROM apache/airflow:3.3.2

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    openpyxl \
    python-dotenv

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dataset/ /opt/airflow/dataset/

RUN mkdir -p /opt/airflow/output

WORKDIR /opt/airflow
