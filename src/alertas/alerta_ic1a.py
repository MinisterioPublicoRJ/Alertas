#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


proto_columns = ['docu_dk', 'docu_nr_mp', 'docu_orgi_orga_dk_responsavel'] 

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('dt_fim_prazo').alias('alrt_date_referencia'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key'),
    # col('stao_dk').alias('alrt_dk_referencia')
]

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_ic1a(options):
    # documento = spark.sql("from documentos_ativos").\
    #     filter('docu_tpst_dk != 3').\
    #     filter("docu_cldc_dk = 392")
    # # classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    # apenso = spark.table('%s.mcpr_correlacionamento' % options['schema_exadata']).\
    #     filter('corr_tpco_dk in (2, 6)')
    # vista = spark.sql("from vista")
    # andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).\
    #     filter('pcao_dt_cancelamento IS NULL')
    # sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).\
    #     filter('stao_tppr_dk in (6012, 6002, 6511, 6291)')
    # orgao_carga = spark.table('%s.orgi_orgao' % options['schema_exadata']).\
    #     filter("orgi_nm_orgao LIKE '%GRUPO DE ATUAÇÃO%'")
    # #tp_andamento = spark.table('%s.mmps_tp_andamento' % options['schema_exadata_aux'])

    # doc_apenso = documento.join(apenso, documento.DOCU_DK == apenso.CORR_DOCU_DK2, 'left').\
    #     filter('corr_tpco_dk is null')
    # # doc_classe = doc_apenso.join(broadcast(classe), doc_apenso.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    # doc_orgao = doc_apenso.join(broadcast(orgao_carga), doc_apenso.DOCU_ORGI_ORGA_DK_CARGA == orgao_carga.ORGI_DK, 'left').\
    #     filter('orgi_dk is null')
    # doc_vista = doc_orgao.join(vista, doc_orgao.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    # doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    # doc_sub_andamento = doc_andamento.join(sub_andamento, doc_andamento.PCAO_DK == sub_andamento.STAO_PCAO_DK, 'inner')
    # #doc_sub_andamento = doc_sub_andamento.join(tp_andamento, doc_sub_andamento.STAO_TPPR_DK == tp_andamento.ID, 'inner')
    
    # doc_eventos = doc_sub_andamento.\
    #     groupBy(proto_columns).agg({'pcao_dt_andamento': 'max'}).\
    #     withColumnRenamed('max(pcao_dt_andamento)', 'last_date')

    # doc_desc_movimento = doc_eventos.join(
    #         doc_sub_andamento.select(['VIST_DOCU_DK', 'PCAO_DT_ANDAMENTO', 'STAO_DK']),
    #         doc_eventos.docu_dk == doc_sub_andamento.VIST_DOCU_DK
    #     ).\
    #     filter('last_date = pcao_dt_andamento').\
    #     groupBy(proto_columns + ['last_date']).agg({'STAO_DK': 'max'}).\
    #     withColumnRenamed('max(STAO_DK)', 'STAO_DK')

    # resultado = doc_desc_movimento.\
    #     withColumn('dt_fim_prazo', expr("to_timestamp(date_add(last_date, 365), 'yyyy-MM-dd HH:mm:ss')")).\
    #     withColumn('elapsed', lit(datediff(current_date(), 'dt_fim_prazo')).cast(IntegerType()))
    #     #withColumn('dt_fim_prazo', expr('date_add(last_date, 365)')).\

    # resultado = resultado.filter('elapsed > 0')

    ANDAMENTO_PRORROGACAO = 6291
    ANDAMENTO_INSTAURACAO = (6511, 6012, 6002)
    ANDAMENTOS_TOTAL = (ANDAMENTO_PRORROGACAO,) + ANDAMENTO_INSTAURACAO

    resultado = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel,
            to_timestamp(date_add(dt_inicio, nr_dias_prazo), 'yyyy-MM-dd HH:mm:ss') as dt_fim_prazo,
            (datediff(current_timestamp(), dt_inicio) - nr_dias_prazo) as elapsed
        FROM
        (
            SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel,
            CASE WHEN MAX(dt_instauracao) IS NOT NULL THEN MAX(dt_instauracao) ELSE docu_dt_cadastro END AS dt_inicio,
            365*(1 + SUM(nr_prorrogacoes)) as nr_dias_prazo
            FROM 
            (
                SELECT docu_dk, docu_nr_mp, docu_dt_cadastro, docu_orgi_orga_dk_responsavel,
                CASE WHEN stao_tppr_dk IN {ANDAMENTO_INSTAURACAO} THEN pcao_dt_andamento ELSE NULL END as dt_instauracao,
                CASE WHEN stao_tppr_dk = {ANDAMENTO_PRORROGACAO} THEN 1 ELSE 0 END AS nr_prorrogacoes
                FROM documentos_ativos
                LEFT JOIN (SELECT * FROM {schema_exadata}.mcpr_correlacionamento WHERE corr_tpco_dk in (2, 6)) C ON C.corr_docu_dk2 = docu_dk
                LEFT JOIN (SELECT * FROM {schema_exadata}.orgi_orgao WHERE orgi_nm_orgao LIKE '%GRUPO DE ATUAÇÃO%') O ON O.orgi_dk = docu_orgi_orga_dk_carga
                LEFT JOIN (
                    SELECT *
                    FROM vista
                    JOIN {schema_exadata}.mcpr_andamento ON pcao_vist_dk = vist_dk
                    JOIN {schema_exadata}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
                    WHERE pcao_dt_cancelamento IS NULL
                    AND stao_tppr_dk in {ANDAMENTOS_TOTAL}
                ) T ON T.vist_docu_dk = docu_dk
                WHERE docu_cldc_dk = 392
                AND docu_tpst_dk != 3
                AND corr_tpco_dk IS NULL
                AND orgi_dk IS NULL
            ) A
            GROUP BY docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel, docu_dt_cadastro
        ) B
        WHERE datediff(current_timestamp(), dt_inicio) > nr_dias_prazo
    """.format(
            schema_exadata=options['schema_exadata'],
            ANDAMENTO_INSTAURACAO=ANDAMENTO_INSTAURACAO,
            ANDAMENTO_PRORROGACAO=ANDAMENTO_PRORROGACAO,
            ANDAMENTOS_TOTAL=ANDAMENTOS_TOTAL
        )
    )

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))
    
    return resultado.select(columns)
