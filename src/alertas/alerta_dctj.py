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

pre_columns = [
    'docu_dk', 'docu_nr_mp', 'docu_nr_externo', 'docu_tx_etiqueta', 'docu_orgi_orga_dk_responsavel', 
    'cldc_ds_hierarquia', 'cldc_ds_classe'
]

def alerta_dctj(options):
    # documento = spark.table('%s.mcpr_documento' % options['schema_exadata']).\
    #     filter('docu_fsdc_dk = 1')
    documento = spark.sql("from documento").filter('docu_fsdc_dk = 1')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux']).\
        filter("CLDC_DS_HIERARQUIA LIKE 'PROCESSO CRIMINAL%'")
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
    #doc_mp = doc_pessoa.join(mp, doc_pessoa.PESS_NM_PESSOA == mp.alias, 'inner')
    doc_mp = doc_pessoa.alias('doc_pessoa').join(broadcast(mp.alias('mp')), col('doc_pessoa.PESS_NM_PESSOA') == col('mp.alias'), 'inner')
    doc_item = doc_mp.join(item, doc_mp.DOCU_DK == item.ITEM_DOCU_DK, 'inner')
    doc_movimentacao = doc_item.join(movimentacao, doc_item.ITEM_MOVI_DK == movimentacao.MOVI_DK, 'inner')
    doc_promotoria = doc_movimentacao.join(broadcast(interno), doc_movimentacao.MOVI_ORGA_DK_ORIGEM == interno.ORGI_DK, 'inner')
    doc_tribunal = doc_promotoria.join(broadcast(externo), doc_promotoria.MOVI_ORGA_DK_DESTINO == externo.ORGE_ORGA_DK, 'inner').\
        groupBy(pre_columns).agg({'movi_dt_recebimento_guia': 'max'}).\
        withColumnRenamed('max(movi_dt_recebimento_guia)', 'movi_dt_guia')
    
    doc_pre_alerta = doc_tribunal.join(item, doc_tribunal.docu_dk == item.ITEM_DOCU_DK, 'left')
    doc_retorno = doc_pre_alerta.join(
        movimentacao, 
        (doc_pre_alerta.ITEM_MOVI_DK == movimentacao.MOVI_DK) 
            & (doc_pre_alerta.docu_orgi_orga_dk_responsavel == movimentacao.MOVI_ORGA_DK_DESTINO)
            & (doc_pre_alerta.movi_dt_guia < movimentacao.MOVI_DT_RECEBIMENTO_GUIA), 
        'left'
    )
    doc_nao_retornado = doc_retorno.filter('movi_dk is null').\
        withColumn('dt_fim_prazo', expr("to_timestamp(date_add(movi_dt_guia, 60), 'yyyy-MM-dd HH:mm:ss')")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))
        #withColumn('dt_fim_prazo', expr('date_add(movi_dt_guia, 60)')).\

    return doc_nao_retornado.filter('elapsed > 0').select(columns)