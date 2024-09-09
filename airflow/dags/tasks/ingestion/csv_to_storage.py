from google.cloud.storage.client import Client as StorageClient
from airflow.decorators import task

import logging

PROJECT = "karhub-434807"

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


@task
def csv_to_storage(
    local_file_path: str,
    gcs_file_path: str,
    bucket_name: str = "etl-karhub-dados-sp",
):
    """Task responsável pela extração do arquivo .csv local para o bucket na GCP.

    Args:
        local_file_path (str): Caminho para o arquivo dentro da máquina.
        gcs_file_path (str): Caminho para o arquivo dentro do bucket.
        bucket_name (str, optional): Nome do bucket. Defaults to "etl-karhub-dados-sp".
    """

    client = StorageClient(project=PROJECT)
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        blob.upload_from_filename(local_file_path)
        logger.info("Upload do arquivo %s foi realizado", local_file_path)
    except Exception as e:
        logger.error("Erro encontrado: %s", e)
