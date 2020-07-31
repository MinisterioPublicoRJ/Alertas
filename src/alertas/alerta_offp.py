#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('dt_fim_prazo').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados'),
]

def alerta_offp(options):
    # documento = spark.table('%s.mcpr_documento' % options['schema_exadata']).\
    #     filter('docu_tpst_dk != 11').\
    #     filter('docu_fsdc_dk = 1')
    documento = spark.sql("from documento").\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    # vista = spark.table('%s.mcpr_vista' % options['schema_exadata'])
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata'])
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).\
        filter('stao_tppr_dk = 6497')
   
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'left')
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'left')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'left').\
        filter('stao_dk is not null').\
        withColumn('dt_fim_prazo', expr("to_timestamp(date_add(pcao_dt_andamento, 365), 'yyyy-MM-dd HH:mm:ss')")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))

    resultado = doc_sub_andamento.filter('elapsed > 0')
    
    return resultado.select(columns)
