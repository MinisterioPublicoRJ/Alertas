#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, DateType 
from pyspark.sql.functions import *

from base import spark


columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('docu_dt_cadastro').alias('alrt_docu_date'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed').alias('alrt_dias_passados')
]

columns_alias = [
    col('alrt_docu_dk'), 
    col('alrt_docu_nr_mp'), 
    col('alrt_docu_nr_externo'), 
    col('alrt_docu_etiqueta'), 
    col('alrt_docu_classe'),
    col('alrt_docu_date'),  
    col('alrt_orgi_orga_dk'),
    col('alrt_classe_hierarquia')
]

groupby_cols = [
    'alrt_docu_dk'
]

def alerta_prcr(options):
    # data do fato será usada para a maioria dos cálculos
    # Caso a data do fato seja NULL, ou seja maior que a data de cadastro, usar cadastro como data do fato
    doc_pena = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_nr_externo, docu_tx_etiqueta,
            CASE WHEN docu_dt_fato < docu_dt_cadastro THEN docu_dt_fato ELSE docu_dt_cadastro END as docu_dt_fato,
            docu_dt_cadastro, docu_orgi_orga_dk_responsavel, cldc_dk, cldc_ds_classe,
            cldc_ds_hierarquia, id, max_pena, nome_delito, multiplicador, abuso_menor
        FROM documento
        LEFT JOIN {0}.mmps_classe_hierarquia ON cldc_dk = docu_cldc_dk
        JOIN {1}.mcpr_assunto_documento ON docu_dk = asdo_docu_dk
        JOIN {0}.tb_penas_assuntos ON id = asdo_assu_dk
        JOIN {0}.atualizacao_pj_pacote ON docu_orgi_orga_dk_responsavel = id_orgao
        WHERE docu_tpst_dk != 11
        AND docu_fsdc_dk = 1
        AND docu_dt_cadastro >= '2010-01-01'
        AND max_pena IS NOT NULL
    """.format(options['schema_exadata_aux'], options['schema_exadata'])
    )
    doc_pena.createOrReplaceTempView('DOC_PENA')
    
    # Calcula tempos de prescrição a partir das penas máximas
    # Caso um dos assuntos seja multiplicador, multiplicar as penas pelo fator
    doc_prescricao = spark.sql("""
        WITH PENA_FATORES AS (
            SELECT docu_dk, EXP(SUM(LN(max_pena))) AS fator_pena
            FROM DOC_PENA
            WHERE multiplicador = 1
            GROUP BY docu_dk
        )
        SELECT *,
            CASE
                WHEN max_pena_fatorado < 1 THEN 3
                WHEN max_pena_fatorado < 2 THEN 4
                WHEN max_pena_fatorado < 4 THEN 8
                WHEN max_pena_fatorado < 8 THEN 12
                WHEN max_pena_fatorado < 12 THEN 16
                ELSE 20 END AS tempo_prescricao
        FROM (
            SELECT 
                P.*,
                CASE WHEN fator_pena IS NOT NULL THEN max_pena * fator_pena ELSE max_pena END AS max_pena_fatorado
            FROM DOC_PENA P
            LEFT JOIN PENA_FATORES F ON F.docu_dk = P.docu_dk 
            WHERE multiplicador = 0
        ) t
    """)
    doc_prescricao.createOrReplaceTempView('DOC_PRESCRICAO')

    # Se o acusado tiver < 21 ou >= 70, seja na data do fato ou na data presente, multiplicar tempo_prescricao por 0.5
    doc_prescricao_fatorado = spark.sql("""
        WITH PRESCRICAO_FATORES AS (
            SELECT docu_dk, 0.5 AS fator_prescricao
            FROM (
                SELECT 
                    docu_dk,
                    add_months(pesf_dt_nasc, 21 * 12) AS dt_21,
                    add_months(pesf_dt_nasc, 70 * 12) AS dt_70,
                    docu_dt_fato AS dt_compare
                FROM DOC_PRESCRICAO
                JOIN {0}.mcpr_personagem ON pers_docu_dk = docu_dk
                JOIN {0}.mcpr_pessoa_fisica ON pers_pesf_dk = pesf_pess_dk
                WHERE pers_tppe_dk IN (290, 7, 21, 317, 20, 14, 32, 345, 40, 5)
                ) t
            WHERE NOT (dt_compare >= dt_21 AND current_timestamp() < dt_70)
            GROUP BY docu_dk
        )
        SELECT P.*,
        CASE WHEN fator_prescricao IS NOT NULL THEN tempo_prescricao * fator_prescricao ELSE tempo_prescricao END AS tempo_prescricao_fatorado
        FROM DOC_PRESCRICAO P
        LEFT JOIN PRESCRICAO_FATORES F ON F.docu_dk = P.docu_dk
    """.format(options['schema_exadata']))
    doc_prescricao_fatorado.createOrReplaceTempView('DOC_PRESCRICAO_FATORADO')

    # Calcular data inicial de prescrição

    # Casos em que houve rescisão de acordo de não persecução penal, data inicial é a data do andamento
    spark.sql("""
        SELECT vist_docu_dk, pcao_dt_andamento
        FROM vista
        JOIN {0}.mcpr_andamento ON vist_dk = pcao_vist_dk
        JOIN {0}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
        WHERE stao_tppr_dk = 7920
        AND year_month >= 201901
    """.format(options['schema_exadata'])
    ).createOrReplaceTempView('DOCS_ANPP')

    # Casos de abuso de menor, data inicial é a data de 18 anos do menor,
    # no caso em que o menor tinha menos de 18 na data do fato/cadastro

    # Prioridade de data inicial: data de 18 anos (caso abuso menor), rescisão de acordo ANPP, dt_fato
    dt_inicial = spark.sql("""
        WITH DOCS_ABUSO_MENOR AS (
            SELECT docu_dk,
                CASE WHEN dt_18_anos > docu_dt_fato THEN dt_18_anos ELSE NULL END AS dt_18_anos
            FROM DOC_PRESCRICAO_FATORADO P
            JOIN {0}.mcpr_personagem ON pers_docu_dk = docu_dk
            JOIN (
                SELECT 
                    PF.*,
                    add_months(pesf_dt_nasc, 18*12) AS dt_18_anos
                FROM {0}.mcpr_pessoa_fisica PF
                ) t ON pers_pesf_dk = pesf_pess_dk
            WHERE abuso_menor = 1
            AND pers_tppe_dk IN (3, 13, 18, 6, 248, 290)
        )
        SELECT P.*,
            CASE 
                WHEN dt_18_anos IS NOT NULL THEN dt_18_anos
                WHEN pcao_dt_andamento IS NOT NULL THEN pcao_dt_andamento 
                ELSE docu_dt_fato END AS dt_inicial_prescricao
        FROM DOC_PRESCRICAO_FATORADO P
        LEFT JOIN DOCS_ANPP ON vist_docu_dk = docu_dk
        LEFT JOIN DOCS_ABUSO_MENOR M ON M.docu_dk = P.docu_dk
    """.format(options['schema_exadata']))
    dt_inicial.createOrReplaceTempView('DOCS_DT_INICIAL_PRESCRICAO')

    # Data de prescrição = data inicial de prescrição + tempo de prescrição
    resultado = spark.sql("""
        SELECT
            D.*,
            cast(add_months(dt_inicial_prescricao, tempo_prescricao_fatorado * 12) as timestamp) AS data_prescricao
        FROM DOCS_DT_INICIAL_PRESCRICAO D
    """).\
        withColumn("elapsed", lit(datediff(current_date(), 'data_prescricao')).cast(IntegerType()))
    resultado.createOrReplaceTempView('TEMPO_PARA_PRESCRICAO')

    LIMIAR_PRESCRICAO_PROXIMA = -7
    subtipos = spark.sql("""
        SELECT T.*,
            CASE
                WHEN elapsed > 0 THEN 2                                 -- prescrito
                WHEN elapsed <= {LIMIAR_PRESCRICAO_PROXIMA} THEN 0      -- nem prescrito nem proximo
                ELSE 1                                                  -- proximo de prescrever
            END AS status_prescricao
        FROM TEMPO_PARA_PRESCRICAO T
    """.format(
            LIMIAR_PRESCRICAO_PROXIMA=LIMIAR_PRESCRICAO_PROXIMA)
    )
    subtipos = subtipos.groupBy(columns[:-1]).agg(min('status_prescricao'), max('status_prescricao')).\
        withColumnRenamed('max(status_prescricao)', 'max_status').\
        withColumnRenamed('min(status_prescricao)', 'min_status')
    subtipos.createOrReplaceTempView('MAX_MIN_STATUS')

    # Os WHEN precisam ser feitos na ordem PRCR1, 2, 3 e depois 4!
    resultado = spark.sql("""
        SELECT T.*,
        CASE
            WHEN min_status = 2 THEN 'PRCR1'    -- todos prescritos
            WHEN min_status = 1 THEN 'PRCR2'    -- todos próximos de prescrever
            WHEN max_status = 2 THEN 'PRCR3'    -- subentende-se min=0 aqui, logo, algum prescrito (mas não todos próximos)
            WHEN max_status = 1 THEN 'PRCR4'    -- subentende-se min=0, logo, nenhum prescrito, mas algum próximo (não todos)
            ELSE NULL                           -- não entra em nenhum caso (será filtrado)
            END AS alrt_sigla
        FROM MAX_MIN_STATUS T
    """)
    resultado = resultado.filter('alrt_sigla IS NOT NULL').select(columns_alias + ['alrt_sigla'])

    return resultado
