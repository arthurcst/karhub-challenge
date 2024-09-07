"""Arquivo responsável pela criação da Dag de processamento."""

from google.oauth2.service_account import Credentials
from airflow.operators.python import PythonOperator
from google.cloud.storage.client import Client as StorageClient
from google.cloud.bigquery import Client as BqClient
from google.cloud import bigquery
from airflow.decorators import task, dag
from datetime import timedelta
from io import BytesIO

import pandas as pd
import datetime
import pendulum
import airflow
import logging
import os

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# Constantes

CREDENTIALS_PATH = "/opt/airflow/credentials/karhub-key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

DAG_ID = os.path.basename(__file__).replace(".py", "")
DAG_OWNER_NAME = "Data Engineering"
SCHEDULE_INTERVAL = None  # Dag com agendamento MANUAL
BUCKET = "etl-karhub-dados-sp"
START_DATE = pendulum.today("UTC").add(days=-1)  # D-1
PROJECT = "karhub-434807"


def on_failure(context):
    return logger.error("Detailed Log: %s", context)


def on_success(context):
    return logger.info("Task finished with success.")


def upload_to_bq(df: pd.DataFrame, table_id: str):
    # Exportação para o BQ
    bq_client = BqClient(project=PROJECT)

    # o Job utilizará a write disposition de write_truncate pois a carga não é incremental nem histórica
    # Se trata de uma tabela estática, logo, sempre que fizermos a carga, iremos reescrevê-la
    job_config = bigquery.LoadJobConfig(
        autodetect=True, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    try:
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
    except Exception as e:
        logger.error("Erro durante o Load Table: %s", e)

    logger.info("Job foi finalizado. A tabela %s foi atualizada.", table_id)


default_args = {
    "owner": DAG_OWNER_NAME,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": START_DATE,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

# Dags - Processing Dag
# Só teremos uma DAG, todo o fluxo de ETL estará aqui, dado que é um contexto só.


@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    description="Dag responsável por realizar a extração dos dados do arquivo .csv para o bucket no GCS.",
    default_view="graph",
    on_success_callback=on_success,
    on_failure_callback=on_failure,
)
def upload_csv_to_gcs():

    @task
    def csv_to_storage(
        local_file_path: str,
        gcs_file_path: str,
        bucket_name: str = "etl-karhub-dados-sp",
    ):
        client = StorageClient(project=PROJECT)
        try:
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(local_file_path)
            logger.info("Upload do arquivo %s foi realizado", local_file_path)
        except Exception as e:
            logger.error("Erro encontrado: %s", e)

    @task
    def storage_to_raw(
        gcs_file_path: str,
        table_name: str,
        dataset_name: str = "gdv_raw",
        bucket_name: str = "etl-karhub-dados-sp",
    ):

        table_id = f"{PROJECT}.{dataset_name}.{table_name}"

        # Leitura do arquivo no GCS
        client = StorageClient(project=PROJECT)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)

        content = blob.download_as_bytes()
        data = BytesIO(content)

        df = pd.read_csv(data, encoding="latin_1")

        # Tratamento básico para o Schema ter qualidade + rastreabilidade da ingestão

        if table_name == "despesas_raw":
            # Essa tabela tem uma coluna vazia no .csv, então removeremos ela aqui.
            # Tendo em vista que pode ser um problema para o autoschema do BigQuery
            df = df.drop(columns=["Unnamed: 3"])

        df = df.drop(df.index[-1])
        df["dt_insert"] = pendulum.today("utc").to_iso8601_string()
        upload_to_bq(df=df, table_id=table_id)

    @task
    def tratamento_despesas():
        raise NotImplementedError

    @task
    def tratamento_receitas():
        raise NotImplementedError

    bucket_name = BUCKET
    dt = datetime.datetime.now()
    dump_date = dt.strftime("%m-%d-%Y")

    despesas_file_path = "/opt/airflow/files/gdvDespesasExcel.csv"
    despesas_gcs_file_path = f"/despesas/gdvDespesas.csv"

    receitas_file_path = "/opt/airflow/files/gdvReceitasExcel.csv"
    receitas_gcs_file_path = f"receitas/gdvReceitas.csv"

    extraction_despesas = csv_to_storage.override(task_id="upload_despesas")(
        despesas_file_path, despesas_gcs_file_path, bucket_name
    )

    extraction_receitas = csv_to_storage.override(task_id="upload_receitas")(
        receitas_file_path, receitas_gcs_file_path, bucket_name
    )
    despesas_export_to_raw = storage_to_raw.override(task_id="despesas_raw")(
        gcs_file_path=despesas_gcs_file_path, table_name="despesas_raw"
    )

    receitas_export_to_raw = storage_to_raw.override(task_id="receitas_raw")(
        gcs_file_path=receitas_gcs_file_path, table_name="receitas_raw"
    )

    extraction_despesas >> despesas_export_to_raw
    extraction_receitas >> receitas_export_to_raw


dag_instance = upload_csv_to_gcs()
