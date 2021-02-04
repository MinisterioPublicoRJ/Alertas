#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType, TimestampType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('docu_dk').alias('alrt_docu_dk'),
    col('docu_nr_mp').alias('alrt_docu_nr_mp'),
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_sigla'),
    col('alrt_key'),
    col('stao_dk').alias('alrt_dk_referencia'),
]

key_columns = [
    col('docu_dk'),
    col('stao_dk')  # Um mesmo documento pode ou não ter tido prorrogação
]

def alerta_ppfp(options):
    documento = spark.sql("from documentos_ativos").\
        filter('docu_tpst_dk != 3').\
        filter('docu_cldc_dk = 395')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    apenso = spark.table('%s.mcpr_correlacionamento' % options['schema_exadata']).\
        filter('corr_tpco_dk in (2, 6)')
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter('pcao_dt_cancelamento IS NULL')
    prorrogacao = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).filter('stao_tppr_dk = 6291')
    instauracao = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).filter('stao_tppr_dk = 6011')
   
    # Documento não pode estar apenso ou anexo
    doc_apenso = documento.join(apenso, documento.DOCU_DK == apenso.CORR_DOCU_DK2, 'left').\
        filter('corr_tpco_dk is null')
    doc_classe = doc_apenso.join(broadcast(classe), doc_apenso.DOCU_CLDC_DK == classe.cldc_dk, 'left')

    doc_andamento = vista.join(andamento, vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')

    doc_prorrogados = doc_andamento.join(prorrogacao, doc_andamento.PCAO_DK == prorrogacao.STAO_PCAO_DK, 'inner')
    doc_instauracao = doc_andamento.join(instauracao, doc_andamento.PCAO_DK == instauracao.STAO_PCAO_DK, 'inner')

    doc_totais = doc_classe.join(doc_instauracao, doc_classe.DOCU_DK == doc_instauracao.VIST_DOCU_DK, 'left')
    # Caso não tenha data de instauração de PP, utilizar data de cadastro
    doc_data_inicial = doc_totais.\
        withColumn('docu_dt_cadastro', when(col('pcao_dt_andamento').isNotNull(), col('pcao_dt_andamento')).otherwise(col('docu_dt_cadastro'))).\
        groupBy(['DOCU_DK', 'DOCU_NR_MP', 'DOCU_ORGI_ORGA_DK_RESPONSAVEL']).agg({'docu_dt_cadastro': 'max'}).\
        withColumnRenamed('max(docu_dt_cadastro)', 'data_inicio').\
        withColumn('elapsed', lit(datediff(current_date(), 'data_inicio')).cast(IntegerType()))

    doc_totais = doc_data_inicial.join(doc_prorrogados, doc_data_inicial.DOCU_DK == doc_prorrogados.VIST_DOCU_DK, 'left')

    # Separar em alertas PPFP e PPPV
    # Documentos fora do prazo (PPFP)
    # Elapsed = dias desde que o prazo venceu
    doc_prorrogado = doc_totais.filter('stao_dk is not null').filter('elapsed > 180').\
        withColumn('elapsed', col('elapsed') - 180)
    doc_nao_prorrogado = doc_totais.filter('stao_dk is null').filter('elapsed > 90').\
        withColumn('elapsed', col('elapsed') - 90)

    # Documentos próximos de vencer (PPPV)
    # Elapsed = dias faltantes para vencer o prazo
    doc_prorrogado_proximo = doc_totais.filter('stao_dk is not null').filter('elapsed > 160 AND elapsed <= 180').\
        withColumn('elapsed', 180 - col('elapsed'))
    doc_nao_prorrogado_proximo = doc_totais.filter('stao_dk is null').filter('elapsed > 70 AND elapsed <= 90').\
        withColumn('elapsed', 90 - col('elapsed'))

    resultado_ppfp = doc_prorrogado.union(doc_nao_prorrogado).\
        withColumn('alrt_sigla', lit('PPFP').cast(StringType())).\
        withColumn('alrt_descricao', lit('Procedimento Preparatório fora do prazo').cast(StringType()))
    resultado_pppv = doc_prorrogado_proximo.union(doc_nao_prorrogado_proximo).\
        withColumn('alrt_sigla', lit('PPPV').cast(StringType())).\
        withColumn('alrt_descricao', lit('Procedimento Preparatório próximo de vencer').cast(StringType()))

    resultado = resultado_ppfp.union(resultado_pppv)

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))
    
    return resultado.select(columns).distinct()
