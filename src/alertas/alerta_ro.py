#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('nr_delegacia').alias('ro_nr_delegacia'),
    col('pip_codigo').alias('alrt_orgi_orga_dk'),
    col('qt_ros_faltantes').alias('ro_qt_ros_faltantes'),
    col('alrt_key'),
    col('max_proc').alias('ro_max_proc'),
    col('cisp_nome_apresentacao').alias('ro_cisp_nome_apresentacao'),
]

key_columns = [
    col('nr_delegacia'),
    col('max_proc')  # A cada novo ro, seria um alerta novo - é o esperado?
]


def alerta_ro(options):
    df = spark.sql("""
    WITH ros_que_faltam AS (
            SELECT
                CAST(substring(proc_numero, 0, 3) AS INTEGER) as nr_delegacia,
                MIN(proc_numero) proc_delegacia_inicial,
                MAX(proc_numero) max_proc,
                substring(MIN(proc_numero), 5, 5) min_serial_delegacia,
                substring(MAX(proc_numero), 5, 5) max_serial_delegacia,
                CAST(substring(MAX(proc_numero), 5, 5) AS INTEGER) qtd_esperada,
                CAST(substring(MAX(proc_numero), 5, 5) AS INTEGER)
                    - COUNT(DISTINCT proc_numero) qt_ros_faltantes,
                COUNT(DISTINCT proc_numero) total_de_ros_recebidos
            FROM {0}.seg_pub_in_pol_procedimento
            WHERE cast(substring(proc_numero, 11, 4) as int) = year(now())
            GROUP BY nr_delegacia
    )
    SELECT * FROM ros_que_faltam rqf
    JOIN {1}.tb_pip_cisp tpc ON rqf.nr_delegacia = tpc.cisp_codigo
    WHERE rqf.qt_ros_faltantes >= 1
    """.format(options["schema_opengeo"], options["schema_exadata_aux"]))
    df = df.withColumn("numero_delegacia", col("numero_delegacia").cast(StringType()))

    df = df.withColumn('alrt_key', uuidsha(*key_columns))

    return df.select(columns)
