import pandas
from dags_util import get_dollar_quotation, upload_to_bq
from google.cloud.bigquery import Client as BqClient
from airflow.decorators import task

import polars as pl
import logging

PROJECT = "karhub-434807"

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def clean_despesas(df: pl.DataFrame) -> pandas.DataFrame:
    dollar = get_dollar_quotation()
    despesas = (
        df.with_columns(
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
    return despesas.to_pandas()  # BQ aceita nativamente um DF Pandas


def get_despesas() -> pl.DataFrame:
    bq_client = BqClient(project=PROJECT)

    # Em caso de um processamento incremental, faríamos a extração apenas
    # do dado do dia em questão.
    # para isso, temos a coluna dt_insert!
    query = f"""
                SELECT *
                FROM `{PROJECT}.gdv_raw.despesas`
            """

    rows = bq_client.query(query)

    raw_despesas = rows.to_dataframe()
    raw_despesas = pl.DataFrame(raw_despesas)

    return raw_despesas


@task
def create_trusted_despesas():
    """
    Task responsável pelo tratamento da tabela Raw de despesas.

    Os dados são extraídos diretamente do BQ.
    O tratamento é feito utilizando Polars.
    """

    despesas = get_despesas()
    despesas = clean_despesas(despesas)

    target_namespace = f"{PROJECT}.gdv_trusted.despesas"

    upload_to_bq(despesas, target_namespace)
