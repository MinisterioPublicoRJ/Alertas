#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *


from base import spark

mov_columns = [
    col('docu_dk'),
    col('docu_nr_mp'),
    col('docu_nr_externo'),
    col('docu_tx_etiqueta'),
    col('cldc_ds_classe'),
    col('cldc_ds_hierarquia'),
    col('docu_orgi_orga_dk_responsavel'),
    col('dt_guia_pre'),
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
]

full_columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('dt_fim_prazo').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados'),
    col('orge_orga_dk').alias('alrt_orgao_baixa_dk'),
    col('orge_nm_orgao').alias('alrt_orgao_baixa_nm'),
]

def alerta_bdpa(options):
    documento = spark.sql("from documento").filter('docu_tpst_dk = 3').filter('docu_fsdc_dk = 1')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    vista = spark.sql("from vista")
    doc_vista = documento.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter(datediff(to_date(lit("2013-01-01")), 'pcao_dt_andamento') <= 0)
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    last_andamento = doc_andamento.\
        groupBy([col('docu_dk'),]).agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'dt_last_andamento').\
        withColumnRenamed('docu_dk', 'last_docu_dk')
    doc_last_andamento = doc_andamento.join(
        last_andamento,
        [
            doc_andamento.DOCU_DK == last_andamento.last_docu_dk,
            doc_andamento.PCAO_DT_ANDAMENTO == last_andamento.dt_last_andamento
        ],
        'inner'
    )
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata'])
    doc_sub_andamento = doc_andamento.join(sub_andamento, doc_last_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    tp_baixa = spark.table('%s.mmps_tp_andamento' % options['schema_exadata_aux']).filter('id_pai in (6363, 6519)')
    doc_baixa = doc_sub_andamento.join(tp_baixa, doc_sub_andamento.STAO_TPPR_DK == tp_baixa.ID, 'inner').\
        filter('stao_nr_dias_prazo IS NOT NULL')
    
    item_mov = spark.table('%s.mcpr_item_movimentacao' % options['schema_exadata']).\
        select([
            col('item_docu_dk'),
            col('item_movi_dk'),
        ])
    movimentacao = spark.table('%s.mcpr_movimentacao'  % options['schema_exadata']).\
        withColumn(
            'movi_dt_guia',
            coalesce(
                col('movi_dt_recebimento_guia'),
                col('movi_dt_envio_guia'),
                col('movi_dt_criacao_guia')
            )
        ).\
        select([
            col('movi_dk'),
            col('movi_dt_guia'),
            col('movi_orga_dk_destino'),
        ])
    orga_policia = spark.table('exadata.mprj_orgao_ext').\
        filter('orge_tpoe_dk IN (60, 61, 68)') # ORGAOS DA POLICIA
    orga_delegacia = spark.table('exadata.mprj_orgao_ext').\
        filter('orge_tpoe_dk IN (60, 61)') # APENAS DELEGACIAS
    
    doc_item = doc_classe.join(item_mov, documento.DOCU_DK == item_mov.ITEM_DOCU_DK, 'inner') 
    doc_mov = doc_item.join(movimentacao, doc_item.ITEM_MOVI_DK == movimentacao.MOVI_DK, 'inner').\
        select(mov_columns + [col('movi_orga_dk_destino'), ])
    last_mov = doc_mov.groupBy(['docu_dk']).agg({'dt_guia_pre': 'max'}).\
        withColumnRenamed('max(dt_guia_pre)', 'dt_guia').\
        withColumnRenamed('docu_dk', 'last_docu_dk')
    
    doc_mov_dest = last_mov.\
        join(
            doc_mov, 
            [
                doc_mov.docu_dk == last_mov.last_docu_dk,
                doc_mov.dt_guia_pre == last_mov.dt_guia,
            ],
            'inner')
    doc_mov_cop = doc_mov_dest.join(orga_policia, doc_mov_dest.movi_orga_dk_destino == orga_policia.ORGE_ORGA_DK)

    doc_lost = doc_mov_cop.join(last_baixa, doc_mov_cop.docu_dk == last_baixa.last_docu_dk, 'inner').\
        withColumn("dt_fim_prazo", expr("date_add(dt_guia, stao_nr_dias_prazo)")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType())).\
        select(full_columns)
    
    dp_mov = doc_mov.join(orga_policia, doc_mov.movi_orga_dk_destino == orga_policia.ORGE_ORGA_DK)
    last_dp_mov = dp_mov.\
        groupBy([
            'docu_dk',
            'orge_orga_dk',
            'orge_nm_orgao',
        ]).agg({'dt_guia_pre': 'max'}).\
        withColumnRenamed('max(dt_guia_pre)', 'dt_guia_dp').\
        withColumnRenamed('docu_dk', 'dp_docu_dk').\
        withColumnRenamed('orge_orga_dk', 'dp_resp_dk').\
        withColumnRenamed('orge_nm_orgao', 'dp_resp_nm')
    doc_dp_lost = doc_lost.join(last_dp_mov, doc_lost.docu_dk == last_dp_mov.dp_docu_dk, 'left')

    return doc_lost.filter('elapsed > 0').select(columns)
