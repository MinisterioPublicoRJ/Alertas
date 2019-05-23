#-*-coding:utf-8-*-
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
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia')
]

def alerta_vadf():
    documento = spark.table('exadata.mcpr_documento')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    vista = spark.table('exadata.mcpr_vista')
   
    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')

    return doc_classe.join(vista, vista.vist_docu_dk == doc_classe.docu_dk, 'inner').\
        filter('docu_fsdc_dk != 1').\
        filter('docu_tpst_dk != 11').\
        filter('vist_dt_fechamento_vista IS NULL').\
        select(columns)
