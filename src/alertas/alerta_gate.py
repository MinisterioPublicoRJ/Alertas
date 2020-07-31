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
    col('dt_it').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados'),
]

pre_columns = [
    'docu_dk', 'docu_nr_mp', 'docu_nr_externo', 'docu_tx_etiqueta', 
    'cldc_ds_classe', 'docu_orgi_orga_dk_responsavel', 'cldc_ds_hierarquia'
]



def alerta_gate(options):
    # documento = spark.table('%s.mcpr_documento' % options['schema_exadata'])
    documento = spark.sql("from documento")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    # vista = spark.table('%s.mcpr_vista' % options['schema_exadata'])
    vista = spark.sql("from vista")
    instrucao = spark.table('%s.gate_info_tecnica' % options['schema_exadata'])

    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_instrucao = doc_classe.join(broadcast(instrucao), documento.DOCU_DK == instrucao.ITCN_DOCU_DK, 'inner')
    doc_vista = doc_instrucao.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'left')

    doc_sem_vista = doc_vista.filter('vist_dk is null')
    doc_vista_anterior = doc_vista.filter(datediff('ITCN_DT_CADASTRO', 'vist_dt_abertura_vista') <= 1)
    doc_inst_recente = doc_sem_vista.union(doc_vista_anterior)

    resultado = doc_inst_recente.groupBy(pre_columns).agg({'ITCN_DT_CADASTRO': 'min'}).\
        withColumnRenamed('min(ITCN_DT_CADASTRO)', 'dt_it').\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_it')).cast(IntegerType()))

    return resultado.select(columns)