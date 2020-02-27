#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from decouple import config
from base import spark

schema_exadata = config('SCHEMA_EXADATA')
schema_exadata_aux = config('SCHEMA_EXADATA_AUX')

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
    documento = spark.table('%s.mcpr_documento' % schema_exadata).\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('docu_cldc_dk = 393')
    classe = spark.table('%s.mmps_classe_hierarquia' % schema_exadata_aux)
    vista = spark.table('%s.mcpr_vista' % schema_exadata)
    andamento = spark.table('%s.mcpr_andamento' % schema_exadata)
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % schema_exadata).\
        filter('stao_tppr_dk in (6034, 6631, 7751, 7752)')
    

    doc_classe = documento.join(classe, documento.DOCU_CLDC_DK == classe.CLDC_DK, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    doc_autuado = doc_sub_andamento.\
        groupBy(aut_col).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'data_autuacao').\
        withColumnRenamed('vist_dk', 'id_vista')

    doc_revisado = doc_autuado.join(vista, doc_autuado.DOCU_DK == vista.VIST_DOCU_DK, 'inner').\
        filter('data_autuacao < vist_dt_abertura_vista').\
        filter('id_vista != vist_dk')
    # Não é possível dar prosseguimento, pois não há informações de RH no BDA, de modo
    # a avaliar se a vista foi aberta por membro ativo da casa (de modo a contar os 120 dias)