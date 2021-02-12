#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


aut_col = [
    'docu_dk', 'docu_nr_mp', 'docu_orgi_orga_dk_responsavel',
]

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('data_autuacao').alias('alrt_date_referencia'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key')
]

key_columns = [
    col('docu_dk'),
    col('data_autuacao')
]

def alerta_nf30(options):
    # documento = spark.sql("from documentos_ativos").\
    #     filter('docu_cldc_dk = 393')
    # vista = spark.sql("from vista")
    # andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
    #     filter('pcao_dt_cancelamento IS NULL')
    # sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).\
    #     filter('stao_tppr_dk in (6011, 6012, 6013, 6014, 6251, 6252, 6253, 6259, 6260, 6516, 6533, 6556, 6567, 6628, 6034, 6631, 7751, 7752, 6035, 7754, 7753, 6007, 6632, 6291, 7282, 7283)')

    # vistas_andamento = vista.join(andamento, vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    # vistas_andamento = vistas_andamento.join(sub_andamento, vistas_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')

    # conversao = vistas_andamento.filter('stao_tppr_dk in (6011, 6012, 6013, 6014, 6251, 6252, 6253, 6259, 6260, 6322, 6516, 6533, 6556, 6567, 6628)')
    # autuacao = vistas_andamento.filter('stao_tppr_dk in (6034, 6631, 7751, 7752, 6035, 7754, 7753, 6007, 6632)')
    # prorrogacao = vistas_andamento.filter('stao_tppr_dk in (6291, 7282, 7283)')

    # docs_sem_conversao = documento.join(conversao, documento.DOCU_DK == conversao.VIST_DOCU_DK, 'left').\
    #     filter('vist_docu_dk IS NULL').select(aut_col + ['docu_dt_cadastro'])

    # docs_autuacao = docs_sem_conversao.join(autuacao, docs_sem_conversao.docu_dk == autuacao.VIST_DOCU_DK, 'left').\
    #     withColumn('dt_inicio', when(col('pcao_dt_andamento').isNotNull(), col('pcao_dt_andamento')).otherwise(col('docu_dt_cadastro')))
    # docs_autuacao = docs_autuacao.\
    #     groupBy(aut_col).agg({'dt_inicio': 'max'}).\
    #     withColumnRenamed('max(dt_inicio)', 'data_autuacao')

    # docs_prorrogacao = docs_autuacao.join(prorrogacao, docs_autuacao.docu_dk == prorrogacao.VIST_DOCU_DK, 'left')
    # docs_com_prorrogacao = docs_prorrogacao.filter('vist_docu_dk is not null')
    # docs_sem_prorrogacao = docs_prorrogacao.filter('vist_docu_dk is null')

    # resultado_com_prorrogacao = docs_com_prorrogacao.\
    #     withColumn('elapsed', lit(datediff(current_date(), 'data_autuacao')).cast(IntegerType())).\
    #     filter('elapsed > 120')
    # resultado_sem_prorrogacao = docs_sem_prorrogacao.\
    #     withColumn('elapsed', lit(datediff(current_date(), 'data_autuacao')).cast(IntegerType())).\
    #     filter('elapsed > 30')
    # resultado = resultado_com_prorrogacao.union(resultado_sem_prorrogacao)

    ANDAMENTOS_CONVERSAO = (6011, 6012, 6013, 6014, 6251, 6252, 6253, 6259, 6260, 6516, 6533, 6556, 6567, 6628)
    ANDAMENTOS_PRORROGACAO = (6291, 7282, 7283)
    ANDAMENTOS_AUTUACAO = (6034, 6631, 7751, 7752, 6035, 7754, 7753, 6007, 6632)
    ANDAMENTOS_TOTAL = ANDAMENTOS_CONVERSAO + ANDAMENTOS_PRORROGACAO + ANDAMENTOS_AUTUACAO

    resultado = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel, dt_inicio as data_autuacao, datediff(current_timestamp(), dt_inicio) as elapsed
        FROM
        (
            SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel,
            CASE WHEN MAX(dt_autuacao) IS NOT NULL THEN MAX(dt_autuacao) ELSE docu_dt_cadastro END AS dt_inicio,
            MAX(nr_dias_prazo) as nr_dias_prazo
            FROM 
            (
                SELECT docu_dk, docu_nr_mp, docu_dt_cadastro, docu_orgi_orga_dk_responsavel,
                CASE WHEN stao_tppr_dk IN {ANDAMENTOS_AUTUACAO} THEN pcao_dt_andamento ELSE NULL END as dt_autuacao,
                CASE WHEN stao_tppr_dk IN {ANDAMENTOS_CONVERSAO} THEN 1 ELSE 0 END as flag_conversao,
                CASE WHEN stao_tppr_dk IN {ANDAMENTOS_PRORROGACAO} THEN 120 ELSE 30 END AS nr_dias_prazo
                FROM documentos_ativos
                LEFT JOIN (
                    SELECT * FROM
                    vista
                    JOIN {schema_exadata}.mcpr_andamento ON pcao_vist_dk = vist_dk
                    JOIN {schema_exadata}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
                    WHERE pcao_dt_cancelamento IS NULL
                    AND stao_tppr_dk in {ANDAMENTOS_TOTAL}
                ) T ON T.vist_docu_dk = docu_dk
                WHERE docu_cldc_dk = 393
            ) A
            GROUP BY docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel, docu_dt_cadastro
            HAVING MAX(flag_conversao) = 0
        ) B
        WHERE datediff(current_timestamp(), dt_inicio) > nr_dias_prazo
    """.format(
            schema_exadata=options['schema_exadata'],
            ANDAMENTOS_AUTUACAO=ANDAMENTOS_AUTUACAO,
            ANDAMENTOS_CONVERSAO=ANDAMENTOS_CONVERSAO,
            ANDAMENTOS_PRORROGACAO=ANDAMENTOS_PRORROGACAO,
            ANDAMENTOS_TOTAL=ANDAMENTOS_TOTAL
        )
    )

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns)
