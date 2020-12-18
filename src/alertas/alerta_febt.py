#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark


columns = [
    col('numero_delegacia').alias('alrt_dk'),
    col('pip_codigo').alias('alrt_orgi_orga_dk'),
]


def alerta_febt(options):
    df = spark.sql("""
    WITH ultimo_ro_enviado AS (
        SELECT
            CAST(substring(proc_numero, 0, 3) AS INTEGER) as numero_delegacia,
            datediff(current_timestamp(), MAX(data_liberacao)) diff_ultimo_envio
        FROM {0}.seg_pub_in_pol_procedimento
        GROUP BY numero_delegacia
    )
    SELECT
        numero_delegacia,
        pip_codigo
    FROM ultimo_ro_enviado ure
    JOIN {1}.tb_pip_cisp tpc ON ure.numero_delegacia = tpc.cisp_codigo
        AND ure.diff_ultimo_envio > 30
    """.format(options["schema_opengeo"], options["schema_exadata_aux"]))

    return df.select(columns)
