from pyspark.sql.functions import *

from shared.base import spark

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
    documento = spark.table('exadata.mcpr_documento')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    item_mov = spark.table('exadata.mcpr_item_movimentacao')
    mov = spark.table('exadata.mcpr_movimentacao')
    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_mov = item_mov.join(mov, item_mov.item_movi_dk == mov.movi_dk, 'inner')

    return doc_classe.join(doc_mov, doc_classe.docu_dk == doc_mov.item_docu_dk, 'inner').\
        filter('docu_tpdc_dk = 119').\
        filter('docu_tpst_dk != 11').\
        filter('item_in_recebimento IS NULL').\
        filter('movi_tpgu_dk == 2').\
        filter('movi_dt_recebimento_guia IS NULL').\
        select(columns)
