#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('id_orgao').alias('alrt_orgi_orga_dk'),
    col('nr_procedimentos').alias('abr1_nr_procedimentos'),
    col('alrt_key'),
    col('ano_mes').alias('abr1_ano_mes')

]

key_columns = [
    col('ano_mes')
]


def alerta_abr1(options):
    #  cria este alerta para todos os meses se estiver em ambiente de DEV
    if options["schema_exadata_aux"].endswith("_dev"):
        months = "%s" % ",".join(str(i) for i in range(1, 13))
    else:
        months = "4"

    procedimentos = spark.sql("""
        SELECT 
            docu_orgi_orga_dk_responsavel, docu_nr_mp, docu_dt_cadastro, docu_dk
        FROM documentos_ativos
        WHERE datediff(last_day(now()), docu_dt_cadastro) / 365.2425 > 1
            AND docu_dt_cancelamento IS NULL
            AND docu_cldc_dk IN (51219, 51220, 51221, 51222, 51223, 392, 395)
            AND docu_tpst_dk != 3
            AND (
                year(current_date()) = 2020 AND month(current_date()) = 11
                OR month(current_date()) IN ({months})
            )
    """.format(schema_exadata=options["schema_exadata"], months=months))
    procedimentos.createOrReplaceTempView("procedimentos")

    df = spark.sql("""
    SELECT
        docu_orgi_orga_dk_responsavel AS id_orgao,
        COUNT(1) AS nr_procedimentos,
        concat_ws('', year(current_date()), month(current_date())) as ano_mes
    FROM procedimentos
    INNER JOIN {schema_aux}.atualizacao_pj_pacote pac ON pac.id_orgao = docu_orgi_orga_dk_responsavel
	    AND UPPER(orgi_nm_orgao) LIKE '%TUTELA%'
    GROUP BY docu_orgi_orga_dk_responsavel
    """.format(schema_aux=options["schema_exadata_aux"]))

    df = df.withColumn('alrt_key', uuidsha(*key_columns))

    procedimentos.write.mode("overwrite").saveAsTable("{}.{}".format(options['schema_alertas'], options['abr1_tabela_aux']))

    return df.select(columns)
