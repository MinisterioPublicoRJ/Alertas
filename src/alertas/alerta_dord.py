#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('pcao_dt_andamento').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia')
]

def alerta_dord():
    documento = spark.table('exadata.mcpr_documento')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    vista = spark.table('exadata.mcpr_vista')
    andamento = spark.table('exadata.mcpr_andamento').filter('pcao_tpsa_dk = 2')
   
    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_vista = doc_classe.join(vista, vista.vist_docu_dk == documento.docu_dk)
    doc_andamento = doc_vista.join(andamento, doc_vista.vist_dk == andamento.pcao_vist_dk)
    last_andamento = doc_andamento.select(['docu_dk', 'pcao_dt_andamento']).\
        groupBy('docu_dk').agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'last_date').\
        withColumnRenamed('docu_dk', 'land_docu_dk')
    check_andamento = doc_andamento.join(
        last_andamento, 
        (doc_andamento.docu_dk == last_andamento.land_docu_dk) & (doc_andamento.pcao_dt_andamento == last_andamento.last_date)
    )

    return check_andamento.\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('vist_orgi_orga_dk != docu_orgi_orga_dk_responsavel').\
        select(columns)
