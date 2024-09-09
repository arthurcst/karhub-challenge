from google.cloud.bigquery import Client as BqClient
from airflow.decorators import task
import pandas
from dags_util import upload_to_bq

import logging

PROJECT = "karhub-434807"

LOG_FORMAT = "%(asctime)s [%(levelname)s]: %(threadName)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def get_consolidated() -> pandas.DataFrame:
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

    return df


@task
def create_consolidated():
    """
    Task responsável por gerar a tabela consolidada.

    Quis fazer em SQL só para ter mais variedade de tipos de Tasks.
    Dessa forma, pude aproveitar um pouco da vantagem de estar conectado num ambiente GCP com Big Query
    """

    target_namespace = f"{PROJECT}.gdv_refined.consolidated"
    consolidated = get_consolidated()

    upload_to_bq(consolidated, target_namespace)
