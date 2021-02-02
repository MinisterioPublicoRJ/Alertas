#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


proto_columns = ['docu_dk', 'docu_nr_mp', 'docu_orgi_orga_dk_responsavel'] 

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('dt_fim_prazo').alias('alrt_date_referencia'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key'),
    col('stao_dk').alias('alrt_dk_referencia')
]

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_ic1a(options):
    documento = spark.sql("from documentos_ativos").\
        filter('docu_tpst_dk != 3').\
        filter("docu_cldc_dk = 392")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    apenso = spark.table('%s.mcpr_correlacionamento' % options['schema_exadata']).\
        filter('corr_tpco_dk in (2, 6)')
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter('pcao_dt_cancelamento IS NULL')
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).\
        filter('stao_tppr_dk in (6012, 6002, 6511, 6291)')
    #tp_andamento = spark.table('%s.mmps_tp_andamento' % options['schema_exadata_aux'])

    doc_apenso = documento.join(apenso, documento.DOCU_DK == apenso.CORR_DOCU_DK2, 'left').\
        filter('corr_tpco_dk is null')
    doc_classe = doc_apenso.join(broadcast(classe), doc_apenso.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    #doc_sub_andamento = doc_sub_andamento.join(tp_andamento, doc_sub_andamento.STAO_TPPR_DK == tp_andamento.ID, 'inner')
    
    doc_eventos = doc_sub_andamento.\
        groupBy(proto_columns).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'last_date')

    doc_desc_movimento = doc_eventos.join(
            doc_sub_andamento.select(['VIST_DOCU_DK', 'PCAO_DT_ANDAMENTO', 'STAO_DK']),
            doc_eventos.docu_dk == doc_sub_andamento.VIST_DOCU_DK
        ).\
        filter('last_date = pcao_dt_andamento').\
        groupBy(proto_columns + ['last_date']).agg({'STAO_DK': 'max'}).\
        withColumnRenamed('max(STAO_DK)', 'STAO_DK')

    resultado = doc_desc_movimento.\
        withColumn('dt_fim_prazo', expr("to_timestamp(date_add(last_date, 365), 'yyyy-MM-dd HH:mm:ss')")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))
        #withColumn('dt_fim_prazo', expr('date_add(last_date, 365)')).\

    resultado = resultado.filter('elapsed > 0')
    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))
    
    return resultado.select(columns)
