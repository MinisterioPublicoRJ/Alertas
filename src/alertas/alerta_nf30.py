#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark

aut_col = [
    'docu_dk', 'docu_nr_mp', 'docu_nr_externo', 'docu_tx_etiqueta', 'vist_dk',
    'docu_orgi_orga_dk_responsavel', 'cldc_ds_classe', 'cldc_ds_hierarquia'
]

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('dt_fim_prazo').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia')
]

def alerta_nf30():
    documento = spark.table('exadata.mcpr_documento').\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('docu_cldc_dk = 393')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    vista = spark.table('exadata.mcpr_vista')
    andamento = spark.table('exadata.mcpr_andamento')
    sub_andamento = spark.table('exadata.mcpr_sub_andamento').\
        filter('stao_tppr_dk in (6034, 6631, 7751, 7752)')
    

    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.docu_dk == vista.vist_docu_dk, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.vist_dk == andamento.pcao_vist_dk, 'inner')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.pcao_dk == sub_andamento.stao_pcao_dk, 'inner')
    doc_autuado = doc_sub_andamento.\
        groupBy(aut_col).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'data_autuacao').\
        withColumnRenamed('vist_dk', 'id_vista')

    doc_revisado = doc_autuado.join(vista, doc_autuado.docu_dk == vista.vist_docu_dk, 'inner').\
        filter('data_autuacao < vist_dt_abertura_vista').\
        filter('id_vista != vist_dk')
    # Não é possível dar prosseguimento, pois não há informações de RH no BDA, de modo
    # a avaliar se a vista foi aberta por membro ativo da casa (de modo a contar os 120 dias)