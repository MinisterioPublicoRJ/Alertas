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

def alerta_pa1a():
    documento = spark.table('exadata.mcpr_documento').\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter("""docu_cldc_dk in (
            51105, 51106, 51107, 51108, 51109, 51110, 51111, 51112, 51113, 51114, 51115, 51116, 51117, 
            51118, 51119, 51120, 51121, 51175, 51176, 51177, 51179, 51180, 51181, 51182, 51183, 51216
        )""")
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    apenso = spark.table('exadata.mcpr_correlacionamento').\
        filter('corr_tpco_dk in (2, 6)')
    vista = spark.table('exadata.mcpr_vista')
    andamento = spark.table('exadata.mcpr_andamento')
    sub_andamento = spark.table('exadata.mcpr_sub_andamento').\
        filter('stao_tppr_dk in (6013, 6291)')
   
    doc_apenso = documento.join(apenso, documento.docu_dk == apenso.CORR_DOCU_DK2, 'left').\
        filter('corr_tpco_dk is null')
    doc_classe = documento.join(classe, doc_apenso.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.docu_dk == vista.vist_docu_dk, 'left')
    doc_andamento = doc_vista.join(andamento, doc_vista.vist_dk == andamento.pcao_vist_dk, 'left')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.pcao_dk == sub_andamento.stao_pcao_dk, 'left')
    
    doc_prorrogado = doc_sub_andamento.filter('stao_dk is not null').\
        withColumn('dt_fim_prazo', expr('date_add(pcao_dt_andamento, 365)')).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))

    doc_nao_prorrogado = doc_sub_andamento.filter('stao_dk is null').\
        withColumn('dt_fim_prazo', expr('date_add(docu_dt_cadastro, 365)')).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))

    resultado = doc_prorrogado.union(doc_nao_prorrogado)
    
    return resultado.filter('elapsed > 0').select(columns)