#-*-coding:utf-8-*-
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType

from base import spark
from utils import uuidsha


columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('alrt_date_referencia'),
    col('alrt_dias_referencia'),
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('alrt_key'),
    col('vist_dk').alias('alrt_vist_dk'),
]

key_columns = [
    col('docu_dk'),
    col('vist_dk')  # A cada nova vista, seria um alerta novo - é o esperado?
]

def alerta_vadf(options):
    documento = spark.sql("from documento")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    vista = spark.sql("from vista")
   
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')

    resultado = doc_classe.join(vista, vista.VIST_DOCU_DK == doc_classe.DOCU_DK, 'inner').\
        filter('docu_fsdc_dk != 1').\
        filter('docu_tpst_dk != 11').\
        filter('vist_dt_fechamento_vista IS NULL')

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))
    resultado = resultado.withColumn('alrt_date_referencia', lit(None).cast(TimestampType()))
    resultado = resultado.withColumn('alrt_dias_referencia', lit(None).cast(IntegerType()))

    return resultado.select(columns)
