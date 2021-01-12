#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('itcn_dk').alias('alrt_itcn_dk'),
    col('docu_dk').alias('alrt_docu_dk'),
    col('docu_nr_mp').alias('alrt_docu_nr_mp'),
    col('itcn_dt_cadastro').alias('alrt_date_referencia'),
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key')
]

key_columns = [
    col('docu_dk'),
    col('itcn_dk')
]


def alerta_gate(options):
    # possivel filter nos documentos?
    documento = spark.sql("from documento")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    vista = spark.sql("select VIST_DOCU_DK, max(VIST_DT_ABERTURA_VISTA) as DT_MAX_VISTA from vista group by VIST_DOCU_DK")
    instrucao = spark.table('%s.gate_info_tecnica' % options['schema_exadata'])

    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_instrucao = doc_classe.join(broadcast(instrucao), doc_classe.DOCU_DK == instrucao.ITCN_DOCU_DK, 'inner')
    doc_vista = doc_instrucao.join(vista, doc_instrucao.DOCU_DK == vista.VIST_DOCU_DK, 'left')

    doc_sem_vista = doc_vista.filter('DT_MAX_VISTA is null')
    doc_vista_anterior = doc_vista.filter('ITCN_DT_CADASTRO > DT_MAX_VISTA')
    resultado = doc_sem_vista.union(doc_vista_anterior).\
        withColumn('elapsed', lit(datediff(current_date(), 'ITCN_DT_CADASTRO')).cast(IntegerType()))

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns)