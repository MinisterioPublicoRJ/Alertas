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

def alerta_vadf(options):
    # documento = spark.table('%s.mcpr_documento' % options['schema_exadata'])
    documento = spark.sql("from documento")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    # vista = spark.table('%s.mcpr_vista' % options['schema_exadata'])
    vista = spark.sql("from vista")
   
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')

    return doc_classe.join(vista, vista.VIST_DOCU_DK == doc_classe.DOCU_DK, 'inner').\
        filter('docu_fsdc_dk != 1').\
        filter('docu_tpst_dk != 11').\
        filter('vist_dt_fechamento_vista IS NULL').\
        select(columns)
