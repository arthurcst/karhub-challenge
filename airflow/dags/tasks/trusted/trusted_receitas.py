from google.cloud.bigquery import Client as BqClient
from airflow.decorators import task
from dags_util import get_dollar_quotation, upload_to_bq

import polars as pl
import logging

PROJECT = "karhub-434807"

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


@task
def tratamento_receitas():
    """
    Task responsável pelo tratamento da tabela Raw de receitas.

    Os dados são extraídos diretamente do BQ.
    O tratamento é feito utilizando Polars.
    """

    bq_client = BqClient(project=PROJECT)
    namespace = f"{PROJECT}.gdv_raw.receitas"

    # Em caso de um processamento incremental, faríamos a extração apenas
    # do dado do dia em questão.
    # para isso, temos a coluna dt_insert!
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
