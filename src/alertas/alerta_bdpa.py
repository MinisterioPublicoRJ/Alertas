#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


mov_columns = [
    col('docu_dk'),
    col('docu_nr_mp'),
    col('docu_nr_externo'),
    col('docu_tx_etiqueta'),
    col('cldc_ds_classe'),
    col('cldc_ds_hierarquia'),
    col('docu_orgi_orga_dk_responsavel'),
    col('movi_orga_dk_destino'),
]

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

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_bdpa(options):
    documento = spark.sql("from documento").\
        filter('docu_tpst_dk = 3').\
        filter('docu_fsdc_dk = 1')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    item_mov = spark.table('%s.mcpr_item_movimentacao' % options['schema_exadata'])
    movimentacao = spark.table('%s.mcpr_movimentacao'  % options['schema_exadata'])
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata'])
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata'])
    tp_baixa = spark.table('%s.mmps_tp_andamento' % options['schema_exadata_aux']).\
        filter('id_pai in (6363, 6519)')
    orga_policia = spark.table('exadata.mprj_orgao_ext').\
        filter('orge_tpoe_dk IN (54, 60, 61, 68)')

    mov_cop = movimentacao.join(orga_policia, movimentacao.MOVI_ORGA_DK_DESTINO == orga_policia.ORGE_ORGA_DK)
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_item = doc_classe.join(item_mov, documento.DOCU_DK == item_mov.ITEM_DOCU_DK, 'inner')
    doc_mov = doc_item.join(mov_cop, doc_item.ITEM_MOVI_DK == mov_cop.MOVI_DK, 'inner').\
        withColumn(
            'dt_guia_pre',
            coalesce(
                col('movi_dt_recebimento_guia'),
                col('movi_dt_envio_guia'),
                col('movi_dt_criacao_guia')
            )
        ).\
        groupBy(mov_columns).agg({'dt_guia_pre': 'max'}).\
        withColumnRenamed('max(dt_guia_pre)', 'dt_guia')
    
    doc_vista = documento.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    last_andamento = doc_andamento.\
        groupBy([col('docu_dk'),]).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'dt_last_andamento').\
        withColumnRenamed('docu_dk', 'last_docu_dk')
    doc_sub_andamento = doc_andamento.join(
        sub_andamento,
        doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK,
        'inner'
    )
    doc_baixa = doc_sub_andamento.join(tp_baixa, doc_sub_andamento.STAO_TPPR_DK == tp_baixa.ID, 'inner').\
        filter('stao_nr_dias_prazo IS NOT NULL')
    last_baixa = doc_baixa.\
        join(
            last_andamento,
            [
                doc_baixa.DOCU_DK == last_andamento.last_docu_dk,
                doc_baixa.PCAO_DT_ANDAMENTO == last_andamento.dt_last_andamento
            ],
            'inner'
        ).\
        select([
            col('last_docu_dk'),
            col('stao_nr_dias_prazo'),
        ])

    doc_lost = doc_mov.join(last_baixa, doc_mov.docu_dk == last_baixa.last_docu_dk, 'inner').\
        withColumn("dt_fim_prazo", expr("date_add(dt_guia, stao_nr_dias_prazo)")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))

    doc_lost = doc_lost.filter('elapsed > 0').withColumn('alrt_key', uuidsha(*key_columns))

    return doc_lost.select(columns)
