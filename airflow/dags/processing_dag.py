"""Arquivo responsável pela criação da Dag de processamento e suas tasks."""

from google.cloud.storage.client import Client as StorageClient
from google.cloud.bigquery import Client as BqClient
from airflow.decorators import task, dag
from google.cloud import bigquery
from datetime import timedelta
from io import BytesIO

import polars as pl
import pandas as pd
import pendulum
import requests
import logging
import os


LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# Constantes

KEY_PATH = "/opt/airflow/credentials/karhub-key.json"
# Tive problemas com o arquivo .env, então precisei setar manualmente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

DAG_ID = os.path.basename(__file__).replace(".py", "")
START_DATE = pendulum.today("UTC").add(days=-1)  # D-1
DAG_OWNER_NAME = "Data Engineering"
BUCKET = "etl-karhub-dados-sp"
PROJECT = "karhub-434807"
SCHEDULE_INTERVAL = None  # Dag com agendamento MANUAL

# Configurações de libs

# pl.Config(tbl_rows=100, tbl_cols=10, fmt_str_lengths=150, fmt_float="full")


def on_failure(context):
    return logger.error("Detailed Log: %s", context)


def on_success(context):
    return logger.info("Task finished with success.")


def get_dollar_quotation():
    response = requests.get(
        "https://economia.awesomeapi.com.br/json/daily/USD-BRL/?start_date=20220622&end_date=20220622"
    )
    response.raise_for_status()
    data = response.json()

    # Pelo que entendi, no fim do dia, bid foi o preço final de compra
    return float(data.pop().get("bid"))


def upload_to_bq(df: pd.DataFrame, namespace: str):
    # Exportação para o BQ
    bq_client = BqClient(project=PROJECT)

    # o Job utilizará a write disposition de write_truncate pois a carga não é incremental nem histórica
    # Se trata de uma tabela estática, logo, sempre que fizermos a carga, iremos reescrevê-la
    job_config = bigquery.LoadJobConfig(
        autodetect=True, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    try:
        job = bq_client.load_table_from_dataframe(df, namespace, job_config=job_config)
        job.result()
    except Exception as e:
        logger.error("Erro durante o Load Table: %s", e)

    logger.info("Job foi finalizado. A tabela %s foi atualizada.", namespace)


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

        if table_name == "despesas":
            # Essa tabela tem uma coluna vazia no .csv, então removeremos ela aqui.
            # Tendo em vista que pode ser um problema para o autoschema do BigQuery
            df = df.drop(columns=["Unnamed: 3"])

        df = df.drop(df.index[-1])
        df["dt_insert"] = pd.to_datetime(pendulum.now("utc"))
        upload_to_bq(df=df, namespace=table_id)

    @task
    def tratamento_despesas():
        bq_client = BqClient(project=PROJECT)
        namespace = f"{PROJECT}.gdv_raw.despesas"

        query = f"""
                    SELECT *
                    FROM `{namespace}`
                """

        rows = bq_client.query(query)

        data_despesas = rows.to_dataframe()
        data_despesas = pl.DataFrame(data_despesas)

        dollar = get_dollar_quotation()

        despesas = (
            data_despesas.with_columns(
                pl.col("Liquidado")
                .map_elements(
                    lambda x: round(
                        float(x.strip().replace(".", "").replace(",", ".")) * dollar, 2
                    ),
                    return_dtype=pl.Float64,
                )
                .alias("liquidado"),
                pl.col("Fonte de Recursos")
                .str.splitn(" - ", 2)
                .struct.field("field_0")
                .alias("id_fonte_recurso"),
                pl.col("Fonte de Recursos")
                .str.splitn(" - ", 2)
                .struct.field("field_1")
                .alias("nome_fonte_recurso"),
            )
            .filter(~pl.col("Despesa").str.contains("TOTAL"))
            .select(
                pl.col("id_fonte_recurso"),
                pl.col("nome_fonte_recurso"),
                pl.col("Despesa").alias("despesa"),
                pl.col("liquidado"),
                pl.col("dt_insert"),
            )
        )

        target_namespace = f"{PROJECT}.gdv_trusted.despesas"

        upload_to_bq(despesas.to_pandas(), target_namespace)

    @task
    def tratamento_receitas():
        bq_client = BqClient(project=PROJECT)
        namespace = f"{PROJECT}.gdv_raw.receitas"

        query = f"""
                    SELECT *
                    FROM `{namespace}`
                """

        rows = bq_client.query(query)

        data_receitas = rows.to_dataframe()
        data_receitas = pl.DataFrame(data_receitas)

        dollar = get_dollar_quotation()
        receitas = (
            data_receitas.with_columns(
                pl.col("Arrecadado")
                .map_elements(
                    lambda x: round(
                        float(x.strip().replace(".", "").replace(",", ".")) * dollar, 2
                    ),
                    return_dtype=pl.Float64,
                )
                .alias("arrecadado"),
                pl.col("Fonte de Recursos")
                .str.splitn(" - ", 2)
                .struct.field("field_0")
                .alias("id_fonte_recurso"),
                pl.col("Fonte de Recursos")
                .str.splitn(" - ", 2)
                .struct.field("field_1")
                .alias("nome_fonte_recurso"),
            )
            .filter(~pl.col("Receita").str.contains("TOTAL"))
            .select(
                pl.col("id_fonte_recurso"),
                pl.col("nome_fonte_recurso"),
                pl.col("Receita").alias("receita"),
                pl.col("arrecadado"),
                pl.col("dt_insert"),
            )
        )

        target_namespace = f"{PROJECT}.gdv_trusted.receitas"

        upload_to_bq(receitas.to_pandas(), target_namespace)

    @task
    def consolidated_refined():
        bq_client = BqClient(project=PROJECT)

        query = """
            with despesas_agg as (
            select
                id_fonte_recurso,
                nome_fonte_recurso,
                round(sum(liquidado), 2) as total_liquidado
            from `gdv_trusted.despesas`
            group by id_fonte_recurso, nome_fonte_recurso
            ),

            receitas_agg as (
            select nome_fonte_recurso,
            round(sum(arrecadado), 2) as total_arrecadado
            from `gdv_trusted.receitas`
            group by nome_fonte_recurso
            )

            select  d.id_fonte_recurso, d.nome_fonte_recurso,d.total_liquidado, r.total_arrecadado from despesas_agg d
            inner join receitas_agg r on d.nome_fonte_recurso = r.nome_fonte_recurso
            order by d.total_liquidado desc
        """
        rows = bq_client.query(query)
        df = rows.to_dataframe()

        target_namespace = f"{PROJECT}.gdv_refined.consolidated"

        upload_to_bq(df, target_namespace)

    despesas_file_path = "/opt/airflow/files/gdvDespesasExcel.csv"
    despesas_gcs_file_path = f"/despesas/gdvDespesas.csv"

    receitas_file_path = "/opt/airflow/files/gdvReceitasExcel.csv"
    receitas_gcs_file_path = f"receitas/gdvReceitas.csv"

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

    despesas_curated = tratamento_despesas()
    receitas_curated = tratamento_receitas()

    consolidada_refined = consolidated_refined()

    extraction_despesas >> despesas_export_to_raw >> despesas_curated  # type: ignore
    extraction_receitas >> receitas_export_to_raw >> receitas_curated  # type: ignore

    [despesas_curated, receitas_curated] >> consolidada_refined  # type: ignore


dag_instance = processing_dag()
