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
    col('alrt_key'),
    col('alrt_sigla')
]

key_columns = [
    col('docu_dk'),
    col('data_autuacao')
]

def alerta_nf30(options):
    ANDAMENTOS_CONVERSAO = (6011, 6012, 6013, 6014, 6251, 6252, 6253, 6259, 6260, 6516, 6533, 6556, 6567, 6628)
    ANDAMENTOS_PRORROGACAO = (6291, 7282, 7283)
    ANDAMENTOS_AUTUACAO = (6034, 6631, 7751, 7752, 6035, 7754, 7753, 6007, 6632)
    ANDAMENTOS_TOTAL = ANDAMENTOS_CONVERSAO + ANDAMENTOS_PRORROGACAO + ANDAMENTOS_AUTUACAO

    resultado = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel, dt_inicio as data_autuacao, datediff(current_timestamp(), dt_inicio) as elapsed,
        CASE WHEN datediff(current_timestamp(), dt_inicio) > 120 THEN 'NF120' ELSE 'NF30' END AS alrt_sigla
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
                LEFT JOIN (SELECT * FROM {schema_exadata}.mcpr_correlacionamento WHERE corr_tpco_dk in (2, 6)) C ON C.corr_docu_dk2 = docu_dk
                LEFT JOIN (
                    SELECT * FROM
                    vista
                    JOIN {schema_exadata}.mcpr_andamento ON pcao_vist_dk = vist_dk
                    JOIN {schema_exadata}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
                    WHERE pcao_dt_cancelamento IS NULL
                    AND stao_tppr_dk in {ANDAMENTOS_TOTAL}
                ) T ON T.vist_docu_dk = docu_dk
                WHERE docu_cldc_dk = 393
                AND corr_tpco_dk IS NULL
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
