#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('dt_fim_prazo').alias('alrt_date_referencia'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key')
]

proto_columns = [
    'docu_dk', 'docu_nr_mp', 'docu_orgi_orga_dk_responsavel', 'data_inicio'
]

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_pa1a(options):
    ANDAMENTO_PRORROGACAO = 6291
    ANDAMENTO_INSTAURACAO = 6013
    ANDAMENTOS_TOTAL = (ANDAMENTO_PRORROGACAO, ANDAMENTO_INSTAURACAO)

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
                CASE WHEN stao_tppr_dk = {ANDAMENTO_INSTAURACAO} THEN pcao_dt_andamento ELSE NULL END as dt_instauracao,
                CASE WHEN stao_tppr_dk = {ANDAMENTO_PRORROGACAO} THEN 1 ELSE 0 END AS nr_prorrogacoes
                FROM documentos_ativos
                LEFT JOIN (SELECT * FROM {schema_exadata}.mcpr_correlacionamento WHERE corr_tpco_dk in (2, 6)) C ON C.corr_docu_dk2 = docu_dk
                LEFT JOIN (
                    SELECT *
                    FROM vista
                    JOIN {schema_exadata}.mcpr_andamento ON pcao_vist_dk = vist_dk
                    JOIN {schema_exadata}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
                    WHERE pcao_dt_cancelamento IS NULL
                    AND stao_tppr_dk in {ANDAMENTOS_TOTAL}
                ) T ON T.vist_docu_dk = docu_dk
                WHERE docu_cldc_dk IN (51219, 51220, 51221, 51222, 51223)
                AND docu_tpst_dk != 3
                AND corr_tpco_dk IS NULL
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
    
    return resultado.filter('elapsed > 0').select(columns)
