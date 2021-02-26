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
]

key_columns = [
    col('docu_dk'),
    col('dt_fim_prazo')
]

def alerta_ic1a(options):
    ANDAMENTO_PRORROGACAO = 6291
    ANDAMENTO_INSTAURACAO = (6511, 6012, 6002)
    ANDAMENTOS_TOTAL = (ANDAMENTO_PRORROGACAO,) + ANDAMENTO_INSTAURACAO
    TAMANHO_PRAZO = 365

    resultado = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel,
            to_timestamp(date_add(dt_inicio, {TAMANHO_PRAZO}), 'yyyy-MM-dd HH:mm:ss') as dt_fim_prazo,
            (datediff(current_timestamp(), dt_inicio) - {TAMANHO_PRAZO}) as elapsed
        FROM
        (
            SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel,
            CASE WHEN MAX(pcao_dt_andamento) IS NOT NULL THEN MAX(pcao_dt_andamento) ELSE docu_dt_cadastro END AS dt_inicio
            FROM 
            (
                SELECT docu_dk, docu_nr_mp, docu_dt_cadastro, docu_orgi_orga_dk_responsavel, pcao_dt_andamento
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
        WHERE datediff(current_timestamp(), dt_inicio) > {TAMANHO_PRAZO}
    """.format(
            schema_exadata=options['schema_exadata'],
            ANDAMENTO_INSTAURACAO=ANDAMENTO_INSTAURACAO,
            ANDAMENTO_PRORROGACAO=ANDAMENTO_PRORROGACAO,
            ANDAMENTOS_TOTAL=ANDAMENTOS_TOTAL,
            TAMANHO_PRAZO=TAMANHO_PRAZO
        )
    )

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))
    
    return resultado.select(columns)
