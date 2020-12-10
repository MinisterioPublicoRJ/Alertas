#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


aut_col = [
    'docu_dk', 'docu_nr_mp', 'docu_nr_externo', 'docu_tx_etiqueta',
    'docu_orgi_orga_dk_responsavel', 'cldc_ds_classe', 'cldc_ds_hierarquia'
]

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('data_autuacao').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados'),
    col('alrt_key')
]

key_columns = [
    col('docu_dk'),
    col('data_autuacao')
]

def alerta_nf30(options):
    documento = spark.sql("from documento").\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('docu_cldc_dk = 393')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    vista = spark.sql("from vista")
    promotor = spark.table('%s.rh_funcionario' % options['schema_exadata']).\
        filter('cdtipfunc = 1')
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter('pcao_dt_cancelamento IS NULL')
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).\
        filter('stao_tppr_dk in (6034, 6631, 7751, 7752)')
    
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    doc_autuado = doc_sub_andamento.\
        groupBy(aut_col).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'data_autuacao')

    vista_promotor = vista.join(broadcast(promotor), vista.VIST_PESF_PESS_DK_RESP_ANDAM == promotor.PESF_PESS_DK, 'inner')
    doc_revisado = doc_autuado.join(vista_promotor, doc_autuado.docu_dk == vista.VIST_DOCU_DK, 'inner').\
        filter('data_autuacao < vist_dt_abertura_vista').\
        withColumnRenamed('docu_dk', 'reviewed_docu_dk').select(['reviewed_docu_dk'])
    
    doc_nao_revisado = doc_autuado.join(doc_revisado, doc_autuado.docu_dk == doc_revisado.reviewed_docu_dk, 'left').\
        filter('reviewed_docu_dk IS NULL')

    resultado = doc_nao_revisado.\
        withColumn('elapsed', lit(datediff(current_date(), 'data_autuacao')).cast(IntegerType())).\
        filter('elapsed > 120')

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns) 