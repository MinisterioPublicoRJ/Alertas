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

def alerta_dntj():
    documento = spark.table('exadata.mcpr_documento')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia').\
        filter("CLDC_DS_HIERARQUIA NOT LIKE 'PROCESSO CRIMINAL%'")
    personagem = spark.table('exadata.mcpr_personagem').\
        filter("pers_tppe_dk = 7")
    pessoa = spark.table('exadata.mcpr_pessoa')
    mp = spark.table('exadata_aux.mmps_alias')
    item = spark.table('exadata.mcpr_item_movimentacao')
    movimentacao = spark.table('exadata.mcpr_movimentacao')
    interno = spark.table('exadata.orgi_orgao').\
        filter('orgi_tpor_dk = 1')
    externo = spark.table('exadata.mprj_orgao_ext').\
        filter('orge_tpoe_dk in (63, 64, 65, 66, 67, 69, 70, 83)')

    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'inner')
    doc_personagem = doc_classe.join(personagem, doc_classe.docu_dk == personagem.pers_docu_dk, 'inner')
    doc_pessoa = doc_personagem.join(pessoa, doc_personagem.pers_pess_dk == pessoa.pess_dk, 'inner')
    doc_mp = doc_pessoa.join(mp, doc_pessoa.pess_nm_pessoa == mp.ALIAS, 'inner')
    doc_item = doc_mp.join(item, doc_mp.docu_dk == item.item_docu_dk, 'inner')
    doc_movimentacao = doc_item.join(movimentacao, doc_item.item_movi_dk == movimentacao.movi_dk, 'inner')
    doc_promotoria = doc_movimentacao.join(interno, doc_movimentacao.movi_orga_dk_origem == interno.ORGI_DK, 'inner')
    doc_tribunal = doc_promotoria.join(externo, doc_promotoria.movi_orga_dk_destino == externo.ORGE_ORGA_DK, 'inner').\
        groupBy(pre_columns).agg({'movi_dt_recebimento_guia': 'max'}).\
        withColumnRenamed('max(movi_dt_recebimento_guia)', 'movi_dt_guia')
    
    doc_pre_alerta = doc_tribunal.join(item, doc_tribunal.docu_dk == item.item_docu_dk, 'left')
    doc_retorno = doc_pre_alerta.join(
        movimentacao, 
        (doc_pre_alerta.item_movi_dk == movimentacao.movi_dk) 
            & (doc_pre_alerta.docu_orgi_orga_dk_responsavel == movimentacao.movi_orga_dk_destino)
            & (doc_pre_alerta.movi_dt_guia < movimentacao.movi_dt_recebimento_guia), 
        'left'
    )
    doc_nao_retornado = doc_retorno.filter('movi_dk is null').\
        withColumn('dt_fim_prazo', expr('date_add(movi_dt_guia, 120)')).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))

    return doc_nao_retornado.filter('elapsed > 0').select(columns)