from google.cloud.storage.client import Client as StorageClient
from airflow.decorators import task

from dags_util import upload_to_bq

from io import BytesIO

import pandas as pd
import pendulum
import logging

PROJECT = "karhub-434807"

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


@task
def storage_to_raw(
    gcs_file_path: str,
    table_name: str,
    dataset_name: str = "gdv_raw",
    bucket_name: str = "etl-karhub-dados-sp",
):
    """Task responsável por puxar os dados do GCS para o BQ.

    Args:
        gcs_file_path (str): Caminho para o arquivo dentro do bucket.
        table_name (str): Nome da tabela.
        dataset_name (str, optional): Nome do dataset. Defaults to "gdv_raw".
        bucket_name (str, optional): Nome do bucket. Defaults to "etl-karhub-dados-sp".
    """

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
