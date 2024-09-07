# karhub-challenge

Desafio técnico proposto pela KarHub

## Perguntas a serem respondidas com SQL

1.  Quais são as 5 fontes de recursos que mais arrecadaram?

    ```sql
    select * from (
    SELECT id_fonte_recurso,
        nome_fonte_recurso,
        ANY_VALUE(total_arrecadado) as total_arrecadado, -- Aqui sabemos que só possui 1 valor, então o any_value é apenas para a agregação
        DENSE_RANK() OVER (ORDER BY SUM(total_arrecadado) DESC ) as rank_arrecadacao, -- poderiamos usar rank(), row_number() ou dense_rank(), depende da regra de negócio. não foi definido regra para empate, então assumi que não fazia diferença.
    FROM `karhub-434807.gdv_refined.consolidated`
    group by nome_fonte_recurso, id_fonte_recurso
    )
    where rank_arrecadacao <= 5
    order by rank_arrecadacao

    ```

    O Ranking ficou assim:

    | Nome da Fonte                                | Total Arrecadado   |
    | :------------------------------------------- | :----------------- |
    | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR       | R$ 755069075741.17 |
    | RECURSOS VINCULADOS ESTADUAIS                | R$ 269273764451.94 |
    | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR-INTRA | R$ 160756796425.25 |
    | REC.PROPRIO-ADM.IND.-DOT.INIC.CR.SUPL.       | R$ 56511589734.27  |
    | RECURSOS VINCULADOS FEDERAIS                 | R$ 44597592235.13  |

2.  Quais são as 5 fontes de recursos que mais gastaram?

    ```sql
        select * from (
        SELECT id_fonte_recurso,
            nome_fonte_recurso,
            ANY_VALUE(total_liquidado) as total_liquidado, -- Aqui sabemos que só possui 1 valor, então o any_value é apenas para a agregação
            DENSE_RANK() OVER (ORDER BY SUM(total_liquidado) DESC ) as rank_liquidacao, -- poderiamos usar rank(), row_number() ou dense_rank(), depende da regra de negócio. não foi definido regra para empate, então assumi que não fazia diferença.
        FROM `karhub-434807.gdv_refined.consolidated`
        group by nome_fonte_recurso, id_fonte_recurso
        )
        where rank_liquidacao <= 5
        order by rank_liquidacao
    ```

    O ranking:

    | Nome da Fonte                                | Total Arrecadado   |
    | -------------------------------------------- | ------------------ |
    | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR       | R$ 761629054293.4  |
    | RECURSOS VINCULADOS ESTADUAIS                | R$ 265605878631.16 |
    | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR-INTRA | R$ 160672067571.69 |
    | REC.PROPRIO-ADM.IND.-DOT.INIC.CR.SUPL.       | R$ 52974388171.73  |
    | RECURSOS VINCULADOS FEDERAIS                 | R$ 42029087040.35  |

3.  Quais são as 5 fontes de recursos com a melhor margem bruta?

    ```sql
            select * from (
        SELECT id_fonte_recurso,
            nome_fonte_recurso,
            ANY_VALUE(total_arrecadado - total_liquidado) as margem_bruta , -- Aqui sabemos que só possui 1 valor, então o any_value é apenas para a agregação
            DENSE_RANK() OVER (ORDER BY SUM(total_arrecadado - total_liquidado) DESC ) as rank_margem, -- poderiamos usar rank(), row_number() ou dense_rank(), depende da regra de negócio. não foi definido regra para empate, então assumi que não fazia diferença.
        FROM `karhub-434807.gdv_refined.consolidated`
        group by nome_fonte_recurso, id_fonte_recurso
        )
        where rank_margem <= 5
        order by rank_margem
    ```

4.  Quais são as 5 fontes de recursos que ~~menir~~ menos arrecadaram?

5.  Quais são as 5 fontes de recursos que ~~menir~~ menos gastaram?

6.  Quais são as 5 fontes de recursos com a pior margem bruta?

7.  Qual a média de arrecadação por fonte de recurso?

8.  Qual a média de gastos por fonte de recurso?
