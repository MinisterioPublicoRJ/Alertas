#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from decouple import config
from base import spark

schema_exadata = config('SCHEMA_EXADATA')
schema_exadata_aux = config('SCHEMA_EXADATA_AUX')

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('docu_dt_cadastro').alias('alrt_docu_date'),  
    col('movi_orga_dk_destino').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia')
]

def alerta_ouvi():
    documento = spark.table('%s.mcpr_documento' % schema_exadata)
    classe = spark.table('%s.mmps_classe_hierarquia' % schema_exadata_aux)
    item_mov = spark.table('%s.mcpr_item_movimentacao' % schema_exadata)
    mov = spark.table('%s.mcpr_movimentacao' % schema_exadata)
    doc_classe = documento.join(classe, documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_mov = item_mov.join(mov, item_mov.ITEM_MOVI_DK == mov.MOVI_DK, 'inner')

    return doc_classe.join(doc_mov, doc_classe.DOCU_DK == doc_mov.ITEM_DOCU_DK, 'inner').\
        filter('docu_tpdc_dk = 119').\
        filter('docu_tpst_dk != 11').\
        filter('item_in_recebimento IS NULL').\
        filter('movi_tpgu_dk == 2').\
        filter('movi_dt_recebimento_guia IS NULL').\
        select(columns)
