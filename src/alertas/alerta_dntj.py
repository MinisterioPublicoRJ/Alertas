#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('docu_dk').alias('alrt_docu_dk'),
    col('docu_nr_mp').alias('alrt_docu_nr_mp'),
    col('dt_guia_tj').alias('alrt_date_referencia'),
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key')
]

key_columns = [
    col('docu_dk'),
    col('dt_guia_tj')
]

pre_columns = [
    'docu_dk', 'docu_nr_mp', 'docu_orgi_orga_dk_responsavel',
]

def alerta_dntj(options):
    documento = spark.sql("from documentos_ativos")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux']).\
        filter("CLDC_DS_HIERARQUIA NOT LIKE 'PROCESSO CRIMINAL%'")
    personagem = spark.table('%s.mcpr_personagem' % options['schema_exadata']).\
        filter("pers_tppe_dk = 7")
    pessoa = spark.table('%s.mcpr_pessoa' % options['schema_exadata'])
    mp = spark.table('%s.mmps_alias' % options['schema_exadata_aux'])
    item = spark.table('%s.mcpr_item_movimentacao' % options['schema_exadata'])
    movimentacao = spark.table('%s.mcpr_movimentacao' % options['schema_exadata'])
    interno = spark.table('%s.orgi_orgao' % options['schema_exadata']).\
        filter('orgi_tpor_dk = 1')
    externo = spark.table('%s.mprj_orgao_ext' % options['schema_exadata']).\
        filter('orge_tpoe_dk in (63, 64, 65, 66, 67, 69, 70, 83)')

    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'inner')
    doc_personagem = doc_classe.join(personagem, doc_classe.DOCU_DK == personagem.PERS_DOCU_DK, 'inner')
    doc_pessoa = doc_personagem.join(pessoa, doc_personagem.PERS_PESS_DK == pessoa.PESS_DK, 'inner')
    doc_mp = doc_pessoa.alias('doc_pessoa').join(broadcast(mp.alias('mp')), col('doc_pessoa.PESS_NM_PESSOA') == col('mp.alias'), 'inner')
    doc_item = doc_mp.join(item, doc_mp.DOCU_DK == item.ITEM_DOCU_DK, 'inner')
    doc_movimentacao = doc_item.join(movimentacao, doc_item.ITEM_MOVI_DK == movimentacao.MOVI_DK, 'inner')
    doc_promotoria = doc_movimentacao.join(broadcast(interno), doc_movimentacao.MOVI_ORGA_DK_ORIGEM == interno.ORGI_DK, 'inner')
    doc_tribunal = doc_promotoria.join(broadcast(externo), doc_promotoria.MOVI_ORGA_DK_DESTINO == externo.ORGE_ORGA_DK, 'inner').\
        groupBy(pre_columns).agg({'movi_dt_recebimento_guia': 'max'}).\
        withColumnRenamed('max(movi_dt_recebimento_guia)', 'movi_dt_guia')
    
    item_movi = item.join(movimentacao, item.ITEM_MOVI_DK == movimentacao.MOVI_DK, 'inner')
    doc_retorno = doc_tribunal.join(
        item_movi,
        (doc_tribunal.docu_dk == item_movi.ITEM_DOCU_DK)
            & (doc_tribunal.docu_orgi_orga_dk_responsavel == item_movi.MOVI_ORGA_DK_DESTINO)
            & (doc_tribunal.movi_dt_guia < item_movi.MOVI_DT_RECEBIMENTO_GUIA),
        'left'
    )
    doc_nao_retornado = doc_retorno.filter('movi_dk is null').\
        withColumn('dt_guia_tj', expr("to_timestamp(movi_dt_guia, 'yyyy-MM-dd HH:mm:ss')")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_guia_tj')).cast(IntegerType()))

    resultado = doc_nao_retornado.filter('elapsed > 120')

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns)
