from google.cloud.bigquery import Client as BqClient
from airflow.decorators import task
import pandas
from dags_util import get_dollar_quotation, upload_to_bq

import polars as pl
import logging

PROJECT = "karhub-434807"

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def clean_receitas(df: pl.DataFrame) -> pandas.DataFrame:
    dollar = get_dollar_quotation()
    receitas = (
        df.with_columns(
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

    return receitas.to_pandas()  # BQ aceita nativamente um DF Pandas


def get_receitas() -> pl.DataFrame:
    bq_client = BqClient(project=PROJECT)

    # Em caso de um processamento incremental, faríamos a extração apenas
    # do dado do dia em questão.
    # para isso, temos a coluna dt_insert!
    query = f"""
                SELECT *
                FROM `{PROJECT}.gdv_raw.receitas`
            """

    rows = bq_client.query(query)

    raw_receitas = rows.to_dataframe()
    raw_receitas = pl.DataFrame(raw_receitas)

    return raw_receitas


@task
def create_trusted_receitas():
    """
    Task responsável pelo tratamento da tabela Raw de receitas.

    Os dados são extraídos diretamente do BQ.
    O tratamento é feito utilizando Polars.
    """

    receitas = get_receitas()
    receitas = clean_receitas(receitas)

    target_namespace = f"{PROJECT}.gdv_trusted.receitas"

    upload_to_bq(receitas, target_namespace)
