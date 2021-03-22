#-*-coding:utf-8-*-
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType, StringType

from base import spark
from utils import uuidsha


columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('vist_orgi_orga_dk').alias('alrt_orgi_orga_dk').cast(IntegerType()),
    col('alrt_key'),
    col('vist_dk').alias('alrt_dk_referencia'),
]

key_columns = [
    col('docu_dk'),
    col('vist_dk')  # A cada nova vista, seria um alerta novo - é o esperado?
]

def alerta_vadf(options):
    documento = spark.sql("from documento")
    vista = spark.sql("from vista")

    resultado = documento.join(vista, vista.VIST_DOCU_DK == documento.DOCU_DK, 'inner').\
        filter('docu_fsdc_dk != 1').\
        filter('docu_tpst_dk != 11').\
        filter('vist_dt_fechamento_vista IS NULL')

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns)
