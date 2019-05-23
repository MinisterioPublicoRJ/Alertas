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
    col('docu_dt_cadastro').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados')
]

def alerta_ppfp():
    documento = spark.table('exadata.mcpr_documento').\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('docu_cldc_dk = 395').\
        withColumn('elapsed', lit(datediff(current_date(), 'docu_dt_cadastro')).cast(IntegerType()))
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    vista = spark.table('exadata.mcpr_vista')
    andamento = spark.table('exadata.mcpr_andamento')
    sub_andamento = spark.table('exadata.mcpr_sub_andamento').filter('stao_tppr_dk = 6291')
   
    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.docu_dk == vista.vist_docu_dk, 'left')
    doc_andamento = doc_vista.join(andamento, doc_vista.vist_dk == andamento.pcao_vist_dk, 'left')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.pcao_dk == sub_andamento.stao_pcao_dk, 'left')

    doc_prorrogado = doc_sub_andamento.filter('stao_dk is not null').filter('elapsed > 180')
    doc_nao_prorrogado = doc_sub_andamento.filter('stao_dk is null').filter('elapsed > 30')

    resultado = doc_prorrogado.union(doc_nao_prorrogado)
    
    return resultado.select(columns)
