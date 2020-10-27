#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark


columns = [
    col('id_orgao').alias('alrt_orgi_orga_dk'),
    col('n_procedimentos').alias('alrt_dias_passados')
]


def alerta_abr1(options):
    df = spark.sql("""
     WITH procedimentos as (
        SELECT docu_orgi_orga_dk_responsavel
        FROM documento
        WHERE docu_cldc_dk IN (51219, 51220, 51221, 51222, 51223, 392, 395)
            AND datediff(now(), docu_dt_cadastro) / 365.2425 > 1
            AND docu_dt_cancelamento IS NULL
            AND docu_fsdc_dk = 1
            AND NOT docu_tpst_dk = 11
            AND (
                year(current_date()) = 2020 AND month(current_date()) = 11
                OR month(current_date()) = 4
            )
    )
    SELECT docu_orgi_orga_dk_responsavel AS id_orgao, COUNT(1) AS n_procedimentos
    FROM procedimentos
    INNER JOIN {0}.atualizacao_pj_pacote pac ON pac.id_orgao = docu_orgi_orga_dk_responsavel
	AND UPPER(orgi_nm_orgao) LIKE '%TUTELA%'
    GROUP BY docu_orgi_orga_dk_responsavel
    """.format(options["schema_exadata_aux"]))

    return df.select(columns)
