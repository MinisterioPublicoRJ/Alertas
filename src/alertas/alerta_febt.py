#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('numero_delegacia').alias('ro_nr_delegacia'),
    col('pip_codigo').alias('alrt_orgi_orga_dk'),
    col('cisp_nome_apresentacao').alias('ro_cisp_nome_apresentacao'),
    col('alrt_key'),
]

key_columns = [
    col('numero_delegacia'),
    col('ultima_liberacao')
]


def alerta_febt(options):
    df = spark.sql("""
    WITH ultimo_ro_enviado AS (
        SELECT
            CAST(substring(proc_numero, 0, 3) AS INTEGER) as numero_delegacia,
            datediff(current_timestamp(), MAX(data_liberacao)) diff_ultimo_envio,
            MAX(data_liberacao) as ultima_liberacao
        FROM {0}.seg_pub_in_pol_procedimento
        GROUP BY numero_delegacia
    )
    SELECT
        numero_delegacia,
        pip_codigo,
        cisp_nome_apresentacao,
        ultima_liberacao
    FROM ultimo_ro_enviado ure
    JOIN {1}.tb_pip_cisp tpc ON ure.numero_delegacia = tpc.cisp_codigo
        AND ure.diff_ultimo_envio > 30
    """.format(options["schema_opengeo"], options["schema_exadata_aux"]))
    df = df.withColumn("numero_delegacia", col("numero_delegacia").cast(StringType()))

    df = df.withColumn('alrt_key', uuidsha(*key_columns))

    return df.select(columns)
