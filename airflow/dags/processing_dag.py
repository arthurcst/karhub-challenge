"""Arquivo responsável pela criação da Dag de processamento e suas tasks."""

from tasks.refined.refined_consolidated import consolidated_refined
from tasks.trusted.trusted_despesas import tratamento_despesas
from tasks.trusted.trusted_receitas import tratamento_receitas
from tasks.ingestion.csv_to_storage import csv_to_storage
from tasks.raw.storage_to_raw import storage_to_raw

from dags_util import on_failure, on_success
from airflow.decorators import dag
from datetime import timedelta

import pendulum
import logging
import os


LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# Constantes

# Tive problemas com o arquivo .env, então precisei setar manualmente
KEY_PATH = "/opt/airflow/credentials/karhub-key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

DAG_ID = os.path.basename(__file__).replace(".py", "")
START_DATE = pendulum.today("UTC").add(days=-1)  # D-1
DAG_OWNER_NAME = "Data Engineering"
BUCKET = "etl-karhub-dados-sp"
PROJECT = "karhub-434807"
SCHEDULE_INTERVAL = None  # Dag com agendamento MANUAL


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
def processing_dag():
    """Dag responsável por orquestrar o Pipeline de ETL."""

    despesas_file_path = "/opt/airflow/files/gdvDespesasExcel.csv"
    receitas_file_path = "/opt/airflow/files/gdvReceitasExcel.csv"

    despesas_gcs_file_path = f"/despesas/gdvDespesas.csv"
    receitas_gcs_file_path = f"receitas/gdvReceitas.csv"

    # Criação das Tasks

    extraction_despesas = csv_to_storage.override(task_id="upload_despesas")(
        despesas_file_path, despesas_gcs_file_path, BUCKET
    )
    extraction_receitas = csv_to_storage.override(task_id="upload_receitas")(
        receitas_file_path, receitas_gcs_file_path, BUCKET
    )
    despesas_export_to_raw = storage_to_raw.override(task_id="despesas_raw")(
        gcs_file_path=despesas_gcs_file_path, table_name="despesas"
    )

    receitas_export_to_raw = storage_to_raw.override(task_id="receitas_raw")(
        gcs_file_path=receitas_gcs_file_path, table_name="receitas"
    )

    despesas_trusted = tratamento_despesas()
    receitas_trusted = tratamento_receitas()

    consolidada_refined = consolidated_refined()

    extraction_despesas >> despesas_export_to_raw >> despesas_trusted  # type: ignore
    extraction_receitas >> receitas_export_to_raw >> receitas_trusted  # type: ignore

    [despesas_trusted, receitas_trusted] >> consolidada_refined  # type: ignore


dag_instance = processing_dag()
