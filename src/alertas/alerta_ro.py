#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *

from base import spark


columns = [
    col('numero_delegacia').alias('alrt_dk'),
    col('pip_codigo').alias('alrt_orgi_orga_dk'),
    col('qtd_falta').alias('alrt_dias_passados')
]


def alerta_ro(options):
    df = spark.sql("""
    WITH ros_que_faltam AS (
            SELECT
                CAST(substring(proc_numero, 0, 3) AS INTEGER) as numero_delegacia,
                MIN(proc_numero) proc_delegacia_inicial,
                MAX(proc_numero) proc_delegacia_final,
                substring(MIN(proc_numero), 5, 5) min_serial_delegacia,
                substring(MAX(proc_numero), 5, 5) max_serial_delegacia,
                CAST(substring(MAX(proc_numero), 5, 5) AS INTEGER) qtd_esperada,
                CAST(substring(MAX(proc_numero), 5, 5) AS INTEGER)
                    - COUNT(DISTINCT proc_numero) qtd_falta,
                COUNT(DISTINCT proc_numero) total_de_ros_recebidos
            FROM {0}.seg_pub_in_pol_procedimento
            WHERE year(proc_data) = year(now())
            GROUP BY numero_delegacia
    )
    SELECT * FROM ros_que_faltam rqf
    JOIN {1}.tb_pip_cisp tpc ON rqf.numero_delegacia = tpc.cisp_codigo
    WHERE rqf.qtd_falta >= 1
    """.format(options["schema_opengeo"], options["schema_exadata_aux"]))
    df = df.withColumn("numero_delegacia", col("numero_delegacia").cast(StringType()))

    return df.select(columns)
