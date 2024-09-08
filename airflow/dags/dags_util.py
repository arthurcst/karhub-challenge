"""Funções utilitárias para a dag de processamento."""

from google.cloud.bigquery import Client as BqClient
from google.cloud import bigquery

import pandas as pd
import requests
import logging

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


PROJECT = "karhub-434807"


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

    # Pelo que entendi da documentação, no fim do dia, bid foi o preço final de compra
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
