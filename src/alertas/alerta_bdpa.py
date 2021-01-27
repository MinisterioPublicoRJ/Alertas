#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('dt_fim_prazo').alias('alrt_date_referencia').cast(TimestampType()),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('nm_delegacia').alias('alrt_info_adicional'),
    col('alrt_key')
]

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_bdpa(options):
    documento = spark.sql("from documento").filter('DOCU_TPST_DK = 3').filter('DOCU_FSDC_DK = 1')
    orga_externo = spark.table('%s.mprj_orgao_ext' % options['schema_exadata']).\
        withColumnRenamed('ORGE_NM_ORGAO', 'nm_delegacia')
    doc_origem = documento.join(
        orga_externo,
        documento.DOCU_ORGE_ORGA_DK_DELEG_ORIGEM == orga_externo.ORGE_ORGA_DK,
        'left'
    )
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    doc_classe = doc_origem.join(broadcast(classe), doc_origem.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    vista = spark.sql("from vista")
    doc_vista = doc_classe.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
        filter(datediff(to_date(lit("2013-01-01")), 'PCAO_DT_ANDAMENTO') <= 0)
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    last_andamento = doc_andamento.\
        groupBy([col('DOCU_DK'),]).agg({'PCAO_DT_ANDAMENTO': 'max'}).\
        withColumnRenamed('max(PCAO_DT_ANDAMENTO)', 'dt_last_andamento').\
        withColumnRenamed('DOCU_DK', 'last_docu_dk')
    doc_last_andamento = doc_andamento.join(
        last_andamento,
        [
            doc_andamento.DOCU_DK == last_andamento.last_docu_dk,
            doc_andamento.PCAO_DT_ANDAMENTO == last_andamento.dt_last_andamento
        ],
        'inner'
    )
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata'])
    doc_sub_andamento = doc_last_andamento.join(sub_andamento, doc_last_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    tp_baixa = spark.table('%s.mmps_tp_andamento' % options['schema_exadata_aux']).\
        filter('id in (6006, 6010, 6363, 6494, 6495, 6519, 6520, 6521, 6522, 6523)')
    doc_baixa = doc_sub_andamento.join(tp_baixa, doc_sub_andamento.STAO_TPPR_DK == tp_baixa.ID, 'inner').\
        filter('STAO_NR_DIAS_PRAZO IS NOT NULL')
    
    item_mov = spark.table('%s.mcpr_item_movimentacao' % options['schema_exadata'])
    doc_item = doc_baixa.join(item_mov, doc_baixa.DOCU_DK == item_mov.ITEM_DOCU_DK, 'inner') 
    movimentacao = spark.table('%s.mcpr_movimentacao' % options['schema_exadata']).\
        withColumn(
            'movi_dt_guia',
            coalesce(
                col('MOVI_DT_RECEBIMENTO_GUIA'),
                col('MOVI_DT_ENVIO_GUIA'),
                col('MOVI_DT_CRIACAO_GUIA')
            )
        )
    doc_mov = doc_item.join(movimentacao, doc_item.ITEM_MOVI_DK == movimentacao.MOVI_DK, 'inner')  
    
    last_mov = doc_mov.groupBy(['docu_dk']).agg({'movi_dt_guia': 'max'}).\
        withColumnRenamed('max(movi_dt_guia)', 'dt_guia').\
        withColumnRenamed('docu_dk', 'last_mov_docu_dk')
    doc_mov_dest = last_mov.\
        join(
            doc_mov, 
            [
                doc_mov.DOCU_DK == last_mov.last_mov_docu_dk,
                doc_mov.movi_dt_guia == last_mov.dt_guia,
            ],
            'inner')
    
    # ORGAOS DA POLICIA
    orga_policia = orga_externo.filter('ORGE_TPOE_DK IN (60, 61, 68)').\
        withColumnRenamed('nm_delegacia', 'nm_orga_destino').\
        withColumnRenamed('ORGE_ORGA_DK', 'ORGE_ORGA_DK_POLICIA')
    # APENAS DELEGACIAS
    # orga_delegacia = orga_externo.filter('ORGE_TPOE_DK IN (60, 61)')
    doc_mov_cop = doc_mov_dest.join(orga_policia, doc_mov_dest.MOVI_ORGA_DK_DESTINO == orga_policia.ORGE_ORGA_DK_POLICIA)
    doc_lost = doc_mov_cop.withColumn("dt_fim_prazo", expr("date_add(dt_guia, stao_nr_dias_prazo)")).\
        withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType())).\
        filter('elapsed > 0')

    doc_lost = doc_lost.withColumn('alrt_key', uuidsha(*key_columns))

    return doc_lost.select(columns).distinct()
