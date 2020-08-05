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


def get_tempo_prescricao(max_pena):
    # Alguém pode refatorar isso de forma inteligente?
    if max_pena < 1:
        return 3
    elif max_pena < 2:
        return 4
    elif max_pena < 4:
        return 8
    elif max_pena < 8:
        return 12
    elif max_pena < 12:
        return 16
    else:
        return 20


def alerta_prcr(options):
    prescricao_udf = udf(lambda max_pena: get_tempo_prescricao(max_pena), IntegerType())

    spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_nr_externo, docu_tx_etiqueta, docu_dt_fato,
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
        AND cod_pct IN (20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 200)
    """.format(options['schema_exadata_aux'], options['schema_exadata'])
    ).createOrReplaceTempView('DOC_PENA')

    # Aplicar multiplicadores de pena
    pena_fatores = spark.sql("""
        SELECT docu_dk, EXP(SUM(LN(max_pena))) AS fator_pena
        FROM DOC_PENA
        WHERE multiplicador = 1
        GROUP BY docu_dk
        """).createOrReplaceTempView('PENA_FATORES')
    doc_prescricao = spark.sql("""
        SELECT 
            P.*,
            CASE WHEN fator_pena IS NOT NULL THEN max_pena * fator_pena ELSE max_pena END AS max_pena_fatorado
        FROM DOC_PENA P
        LEFT JOIN PENA_FATORES F ON F.docu_dk = P.docu_dk 
        WHERE multiplicador = 0
    """)
    doc_prescricao = doc_prescricao.withColumn("tempo_prescricao", prescricao_udf(doc_prescricao.max_pena_fatorado))
    doc_prescricao.createOrReplaceTempView('DOC_PRESCRICAO')

    # Se o acusado tiver < 21 ou >= 70, multiplicar tempo_prescricao por 0.5
    spark.sql("""
        SELECT 
            docu_dk,
            0.5 AS fator_prescricao
        FROM (
            SELECT 
                docu_dk,
                add_months(pesf_dt_nasc, 21 * 12) AS dt_21,
                add_months(pesf_dt_nasc, 70 * 12) AS dt_70,
                CASE WHEN docu_dt_fato IS NOT NULL THEN docu_dt_fato ELSE docu_dt_cadastro END AS dt_compare
            FROM DOC_PRESCRICAO
            JOIN {0}.mcpr_personagem ON pers_docu_dk = docu_dk
            JOIN {0}.mcpr_pessoa_fisica ON pers_pesf_dk = pesf_pess_dk
            WHERE pers_tppe_dk IN (290, 7, 21, 317, 20, 14, 32, 345, 40, 5)
            ) t
        WHERE NOT (dt_compare >= dt_21 AND dt_compare < dt_70)
        GROUP BY docu_dk
    """.format(options['schema_exadata'])).createOrReplaceTempView('PRESCRICAO_FATORES')

    doc_prescricao = spark.sql("""
        SELECT P.*,
        CASE WHEN fator_prescricao IS NOT NULL THEN tempo_prescricao * fator_prescricao ELSE tempo_prescricao END AS tempo_prescricao_fatorado
        FROM DOC_PRESCRICAO P
        LEFT JOIN PRESCRICAO_FATORES F ON F.docu_dk = P.docu_dk
    """).createOrReplaceTempView('DOC_PRESCRICAO_FATORADO')

    # Calcular data inicial de prescrição

    # Casos em que houve rescisão de acordo de não persecução penal, data inicial é a data do andamento
    spark.sql("""
        SELECT vist_docu_dk, pcao_dt_andamento
        FROM vista
        JOIN {0}.mcpr_andamento ON vist_dk = pcao_vist_dk
        JOIN {0}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
        WHERE stao_tppr_dk = 7920
    """.format(options['schema_exadata'])
    ).createOrReplaceTempView('DOCS_ANPP')

    # Casos de abuso de menor, data inicial é a data de 18 anos do menor,
    # no caso em que o menor tinha menos de 18 na data do fato/cadastro
    spark.sql("""
        SELECT docu_dk,
            CASE 
                WHEN docu_dt_fato IS NOT NULL AND dt_18_anos > docu_dt_fato THEN dt_18_anos
                WHEN docu_dt_fato IS NULL AND dt_18_anos > docu_dt_cadastro THEN dt_18_anos
                ELSE NULL END AS dt_18_anos
        FROM DOC_PRESCRICAO_FATORADO P
        JOIN {0}.mcpr_personagem ON pers_docu_dk = docu_dk
        JOIN (
            SELECT 
                *,
                add_months(pesf_dt_nasc, 18*12) AS dt_18_anos
            FROM {0}.mcpr_pessoa_fisica
            ) t ON pers_pesf_dk = pesf_pess_dk
        WHERE abuso_menor = 1
        AND pers_tppe_dk IN (3, 13)
    """.format(options['schema_exadata'])
    ).createOrReplaceTempView('DOCS_ABUSO_MENOR')

    # Prioridade de data inicial: data de 18 anos (caso abuso menor), rescisão de acordo ANPP, dt_fato, dt_cadastro
    spark.sql("""
        SELECT P.*,
            CASE 
                WHEN dt_18_anos IS NOT NULL THEN dt_18_anos
                WHEN pcao_dt_andamento IS NOT NULL THEN pcao_dt_andamento
                WHEN docu_dt_fato IS NOT NULL THEN docu_dt_fato 
                ELSE docu_dt_cadastro END AS dt_inicial_prescricao
        FROM DOC_PRESCRICAO_FATORADO P
        LEFT JOIN DOCS_ANPP ON vist_docu_dk = docu_dk
        LEFT JOIN DOCS_ABUSO_MENOR M ON M.docu_dk = P.docu_dk
    """.format(options['schema_exadata'])).createOrReplaceTempView('DOCS_DT_INICIAL_PRESCRICAO')

    # Data de prescrição = data inicial de prescrição + tempo de prescrição
    resultado = spark.sql("""
        SELECT
            *,
            add_months(dt_inicial_prescricao, tempo_prescricao_fatorado * 12) AS data_prescricao
        FROM DOCS_DT_INICIAL_PRESCRICAO
    """).\
    withColumn("elapsed", lit(datediff(current_date(), 'data_prescricao')).cast(IntegerType()))
    resultado.createOrReplaceTempView('RESULTADO')
    
    # table_name_teste = "test_alerta_PRCR"
    # resultado.write.mode("overwrite").saveAsTable(table_name_teste)
    # resultado.count()
    resultado = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_nr_externo, docu_tx_etiqueta, cldc_ds_classe,
        docu_dt_cadastro, docu_orgi_orga_dk_responsavel, cldc_ds_hierarquia, elapsed 
        FROM RESULTADO
        WHERE elapsed > 0
        GROUP BY docu_dk, docu_nr_mp, docu_nr_externo, docu_tx_etiqueta, cldc_ds_classe,
        docu_dt_cadastro, docu_orgi_orga_dk_responsavel, cldc_ds_hierarquia, elapsed
    """)

    #resultado = resultado.filter('elapsed > 0').select(columns).distinct()
    resultado.count()

    return resultado
    # Falta o group by para definir qual pena causa a prescrição em processos com multiplos crimes.
    # No momento, o processo está prescrevendo várias vezes.
