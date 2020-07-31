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
    col('elapsed_fato').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados')
]


def get_tempo_prescricao(max_pena):
    # Alguém pode refatorar isso de forma inteligente?
    if max_pena < 1:
        return 3
    elif max_pena < 2:
        return 4
    elif max_pena < 4:
        return 8
    elif max_pena < 8:
        return 12
    elif max_pena < 12:
        return 16
    else:
        return 20


def alerta_prcr(options):
    prescricao_udf = udf(lambda max_pena: get_tempo_prescricao(max_pena), IntegerType())

    documento = spark.sql("from documento").\
        filter("docu_tpst_dk != 11").\
        filter("docu_fsdc_dk = 1").\
        filter("docu_dt_cadastro >= '2010-01-01'")
        # Somente avaliar processos de 2010 pra cá
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    assunto_docto = spark.table('%s.mmps_assunto_docto' % options['schema_exadata_aux'])
    pena = '' #INSERIR AQUI LEITURA DA TABELA DE PENAS

    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_assunto = doc_classe.join(broadcast(assunto), doc_classe.docu_dk == assunto.asdo_docu_dk)
    doc_pena = doc_assunto.join(broadcast(pena), doc_assunto.asdo_assu_dk == pena.id, 'inner')
    doc_pena.withColumn("tempo_prescricao", prescricao_udf(doc_pena.max_pena)).\
        withColumn("data_prescricao", add_months(doc_pena.docu_dt_fato, doc_pena.tempo_prescricao * 12)).\
        withColumn("elapsed", lit(datediff(current_date(), 'data_prescricao')).cast(IntegerType()))
    # Vai ser preciso revisar mais tarde - processos de abuso sexual de menores usam o aniversário
    # de 18 anos da vítima em vez de data do fato. Vamos precisar obter os personagens vítimas 
    # nesses casos e comparar com a data de nascimento mais recente e somar 18 anos.
    # Isso acontece nos processos que relatam aos artigos 240, 241, 241-A a 241-D e 244-A do 
    # ECA (Lei 8069/1990), e aos artigos 213 a 218-C do CP (somente se a vítima tiver menos de 18 anos
    # na data do fato). Boa sorte, guerreiro

    return resultado.filter('elapsed > 0').select(columns).distinct()
    # Falta o group by para definir qual pena causa a prescrição em processos com multiplos crimes.
    # No momento, o processo está prescrevendo várias vezes.
