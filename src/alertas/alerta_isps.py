#-*-coding:utf-8-*-
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
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia')
]

indicadores = {
    'in009': 'Índice de Hidrometação',
    'in013': 'Índice de Perdas de Faturamento',
    'in023': 'Índice de Atendimento Urbano de Água',
    'in049': 'Índice de Perdas na Distribuição',
    'in015': 'Índice de Coleta de Esgoto',
    'in016': 'Índice de Tratamento de Esgoto',
    'in024': 'Índice de Atendimento Urbano de Esgoto Referido',
    'in046': 'Índice de Esgoto Tratado Referido à Água Consumida',
    'in020': 'Taxa de Cobertura de Pavimentação e Meio-Fio na Área Urbana do Município',
    'in021': 'Taxa de Cobertura de Vias Públicas com Redes ou Canais Pluviais Subterrâneos na Área Urbana',
    'in040': 'Parcela de Domicílios em Situação de Risco de Inundação',
    'in041': 'Parcela da População Impactada por Eventos Hidrológicos'
}

def alerta_isps(options):
    options["schema_opengeo"]
    options['schema_exadata_aux']

    ano_referencia = spark.sql("""
            SELECT MAX(ano_referencia) as max_ano
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_agua
        """.format(options["schema_opengeo"])
    )
    ano_referencia.createOrReplaceTempView('ANO_REFERENCIA')

    agua = spark.sql("""
        WITH agregados AS (
            SELECT cod_mun, in009, in013, in023, in049
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_agua
            JOIN ANO_REFERENCIA ON ano_referencia = max_ano
            AND cod_prest IS NULL
        ),
        indicadores AS (
            SELECT A.cod_mun,
                CASE WHEN A.in009 < R.in009 THEN 'Índice de Hidrometação' ELSE NULL END AS ind1,
                CASE WHEN A.in013 > R.in013 THEN 'Índice de Perdas de Faturamento' ELSE NULL END AS ind2,
                CASE WHEN A.in023 < R.in023 THEN 'Índice de Atendimento Urbano de Água' ELSE NULL END AS ind3,
                CASE WHEN A.in049 > R.in049 THEN 'Índice de Perdas na Distribuição' ELSE NULL END AS ind4
            FROM agregados A
            JOIN (SELECT cod_mun, in009, in013, in023, in049 FROM agregados WHERE cod_mun = 33) R ON R.cod_mun != A.cod_mun
        )
        SELECT cod_mun, ind1 as alrt_descricao 
        FROM indicadores
        WHERE ind1 IS NOT NULL
        UNION ALL
        SELECT cod_mun, ind2 as alrt_descricao 
        FROM indicadores
        WHERE ind2 IS NOT NULL
        UNION ALL
        SELECT cod_mun, ind3 as alrt_descricao 
        FROM indicadores
        WHERE ind3 IS NOT NULL
        UNION ALL
        SELECT cod_mun, ind4 as alrt_descricao 
        FROM indicadores
        WHERE ind4 IS NOT NULL
    """.format(options["schema_opengeo"]))
    agua.createOrReplaceTempView('AGUA')


    esgoto = spark.sql("""
        WITH agregados AS (
            SELECT cod_mun, in015, in016, in024, in046
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_esgoto
            JOIN ANO_REFERENCIA ON ano_referencia = max_ano
            AND cod_prest IS NULL
        ),
        indicadores AS (
            SELECT A.cod_mun,
                CASE WHEN A.in015 < R.in015 THEN 'Índice de Coleta de Esgoto' ELSE NULL END AS ind1,
                CASE WHEN A.in016 < R.in016 THEN 'Índice de Tratamento de Esgoto' ELSE NULL END AS ind2,
                CASE WHEN A.in024 < R.in024 THEN 'Índice de Atendimento Urbano de Esgoto Referido' ELSE NULL END AS ind3,
                CASE WHEN A.in046 < R.in046 THEN 'Índice de Esgoto Tratado Referido à Água Consumida' ELSE NULL END AS ind4
            FROM agregados A
            JOIN (SELECT cod_mun, in015, in016, in024, in046 FROM agregados WHERE cod_mun = 33) R ON R.cod_mun != A.cod_mun
        )
        SELECT cod_mun, ind1 as alrt_descricao 
        FROM indicadores
        WHERE ind1 IS NOT NULL
        UNION ALL
        SELECT cod_mun, ind2 as alrt_descricao 
        FROM indicadores
        WHERE ind2 IS NOT NULL
        UNION ALL
        SELECT cod_mun, ind3 as alrt_descricao 
        FROM indicadores
        WHERE ind3 IS NOT NULL
        UNION ALL
        SELECT cod_mun, ind4 as alrt_descricao 
        FROM indicadores
        WHERE ind4 IS NOT NULL
    """.format(options["schema_opengeo"]))
    esgoto.createOrReplaceTempView('ESGOTO')

    # vermelhos_drenagem = spark.sql("""
    #     WITH agregados AS (
    #         SELECT cod_mun, in020, in021, in040, in041
    #         FROM {0}.plataforma_amb_saneamento_snis_info_indic_drenagem
    #         JOIN ANO_REFERENCIA ON ano_referencia = max_ano
    #     ),
    #     indicadores AS (
    #         SELECT A.cod_mun,
    #             CASE WHEN A.in020 < R.in020 THEN 'Taxa de Cobertura de Pavimentação e Meio-Fio na Área Urbana do Município' ELSE false END AS ind1,
    #             CASE WHEN A.in021 > R.in021 THEN 'Taxa de Cobertura de Vias Públicas com Redes ou Canais Pluviais Subterrâneos na Área Urbana' ELSE false END AS ind2,
    #             CASE WHEN A.in040 > R.in040 THEN 'Parcela de Domicílios em Situação de Risco de Inundação' ELSE false END AS ind3,
    #             CASE WHEN A.in041 > R.in041 THEN 'Parcela da População Impactada por Eventos Hidrológicos' ELSE false END AS ind4
    #         FROM agregados A
    #         JOIN (SELECT cod_mun, in020, in021, in040, in041 FROM agregados WHERE cod_mun = 33) R ON R.cod_mun != A.cod_mun
    #     )
    #     SELECT cod_mun, ind1 as alrt_descricao 
    #     FROM indicadores
    #     WHERE ind1 IS NOT NULL
    #     UNION ALL
    #     SELECT cod_mun, ind2 as alrt_descricao 
    #     FROM indicadores
    #     WHERE ind2 IS NOT NULL
    #     UNION ALL
    #     SELECT cod_mun, ind3 as alrt_descricao 
    #     FROM indicadores
    #     WHERE ind3 IS NOT NULL
    #     UNION ALL
    #     SELECT cod_mun, ind4 as alrt_descricao 
    #     FROM indicadores
    #     WHERE ind4 IS NOT NULL
    # """.format(schema_opengeo))

    INDICADORES = spark.sql("""
        SELECT * FROM AGUA
        UNION ALL
        SELECT * FROM ESGOTO
        -- UNION ALL
        -- SELECT * FROM DRENAGEM
    """)
    INDICADORES.createOrReplaceTempView('INDICADORES')

    PROMOTORIA_MUNICIPIO = spark.sql("""
    SELECT
        stack(14,
        944468, 330190, -- ITABORAI 
        400542, 330010, -- ANGRA DOS REIS
        903670, 330070, -- CABO FRIO
        400564, 330170, -- DUQUE DE CAXIAS
        400548, 330455, -- RIO DE JANEIRO
        400532, 330350, -- NOVA IGUAÇU
        400562, 330390, -- PETROPOLIS
        400538, 330330, -- NITEROI
        400552, 330455, -- RIO DE JANEIRO
        400553, 330455, -- RIO DE JANEIRO
        400767, 330580, -- TERESOPOLIS
        400560, 330455, -- RIO DE JANEIRO
        1342457, 330020, -- ARARUAMA
        400652, 330030 -- BARRA DO PIRAI
    ) AS (id_orgao, cod_mun)
    """)
    PROMOTORIA_MUNICIPIO.createOrReplaceTempView("PROMOTORIA_MUNICIPIO")

    resultados = spark.sql("""
        SELECT P.id_orgao as alrt_orgi_orga_dk, I.alrt_descricao
        FROM {0}.atualizacao_pj_pacote P
        JOIN PROMOTORIA_MUNICIPIO M ON M.id_orgao = P.id_orgao
        JOIN INDICADORES I ON I.cod_mun = M.cod_mun
        WHERE cod_pct IN (24, 28, 183)
    """.format(options['schema_exadata_aux']))

    return resultados
