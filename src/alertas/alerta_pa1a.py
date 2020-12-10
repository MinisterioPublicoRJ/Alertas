#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


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
    col('alrt_key')
]

proto_columns = [
    'docu_dk', 'docu_nr_mp', 'docu_nr_externo', 'docu_tx_etiqueta', 'docu_dt_cadastro',
    'cldc_ds_classe', 'docu_orgi_orga_dk_responsavel', 'cldc_ds_hierarquia'
]

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_pa1a(options):
    documento = spark.sql("from documento").\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter("""docu_cldc_dk in (
            51105, 51106, 51107, 51108, 51109, 51110, 51111, 51112, 51113, 51114, 51115, 51116, 51117, 
            51118, 51119, 51120, 51121, 51175, 51176, 51177, 51179, 51180, 51181, 51182, 51183, 51216
        )""")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    apenso = spark.table('%s.mcpr_correlacionamento' % options['schema_exadata']).\
        filter('corr_tpco_dk in (2, 6)')
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter('pcao_dt_cancelamento IS NULL')
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).\
        filter('stao_tppr_dk in (6013, 6291)')
   
    doc_apenso = documento.join(apenso, documento.DOCU_DK == apenso.CORR_DOCU_DK2, 'left').\
        filter('corr_tpco_dk is null')
    doc_classe = doc_apenso.join(broadcast(classe), doc_apenso.DOCU_CLDC_DK == classe.cldc_dk, 'left')

    # Previne que vistas que não estão associadas aos andamentos definidos sejam contabilizadas
    doc_andamento = vista.join(andamento, vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    doc_totais = doc_classe.join(doc_sub_andamento, doc_classe.DOCU_DK == doc_sub_andamento.VIST_DOCU_DK, 'left')

    # Pode ter mais de um andamento. Pegar o mais recente nesse caso.
    doc_last_prorrogacao = doc_totais.groupBy(proto_columns).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'pcao_dt_andamento')
    
    doc_prorrogado = doc_last_prorrogacao.filter('pcao_dt_andamento is not null').\
        withColumn('dt_fim_prazo', expr("to_timestamp(date_add(pcao_dt_andamento, 365), 'yyyy-MM-dd HH:mm:ss')")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))
        #withColumn('dt_fim_prazo', expr('date_add(pcao_dt_andamento, 365)')).\

    doc_nao_prorrogado = doc_last_prorrogacao.filter('pcao_dt_andamento is null').\
        withColumn('dt_fim_prazo', expr('date_add(docu_dt_cadastro, 365)')).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))

    resultado = doc_prorrogado.union(doc_nao_prorrogado)

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))
    
    return resultado.filter('elapsed > 0').select(columns)
