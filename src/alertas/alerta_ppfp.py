#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType
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
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados'),
    col('alrt_sigla'),
    col('alrt_descricao'),
]

def alerta_ppfp(options):
    documento = spark.sql("from documento").\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('docu_cldc_dk = 395').\
        withColumn('elapsed', lit(datediff(current_date(), 'docu_dt_cadastro')).cast(IntegerType()))
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter('pcao_dt_cancelamento IS NULL')
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).filter('stao_tppr_dk = 6291')
   
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')

    doc_andamento = vista.join(andamento, vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    doc_totais = doc_classe.join(doc_sub_andamento, doc_classe.DOCU_DK == doc_sub_andamento.VIST_DOCU_DK, 'left')

    # Separar em alertas PPFP e PPPV
    # Documentos fora do prazo (PPFP)
    doc_prorrogado = doc_totais.filter('stao_dk is not null').filter('elapsed > 180')
    doc_nao_prorrogado = doc_totais.filter('stao_dk is null').filter('elapsed > 30')

    # Documentos próximos de vencer (PPPV)
    doc_prorrogado_proximo = doc_totais.filter('stao_dk is not null').filter('elapsed > 160 AND elapsed <= 180')
    doc_nao_prorrogado_proximo = doc_totais.filter('stao_dk is null').filter('elapsed > 10 AND elapsed <= 30')

    resultado_ppfp = doc_prorrogado.union(doc_nao_prorrogado).\
        withColumn('alrt_sigla', lit('PPFP').cast(StringType())).\
        withColumn('alrt_descricao', lit('Procedimento Preparatório fora do prazo').cast(StringType()))
    resultado_pppv = doc_prorrogado_proximo.union(doc_nao_prorrogado_proximo).\
        withColumn('alrt_sigla', lit('PPPV').cast(StringType())).\
        withColumn('alrt_descricao', lit('Procedimento Preparatório próximo de vencer').cast(StringType()))

    resultado = resultado_ppfp.union(resultado_pppv)
    
    return resultado.select(columns).distinct()
