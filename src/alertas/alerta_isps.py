#-*-coding:utf-8-*-
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from base import spark
from utils import uuidsha


columns = [
    col('alrt_orgi_orga_dk'),
    col('alrt_descricao'),
    col('alrt_classe_hierarquia'),
    col('alrt_key')
]

key_columns = [
    col('alrt_descricao'),
    col('alrt_classe_hierarquia'),
    col('ano_referencia')
]


def alerta_isps(options):
    ano_referencia = spark.sql("""
            SELECT MAX(ano_referencia) as max_ano
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_agua
        """.format(options["schema_opengeo"])
    ).collect()[0]['max_ano']

    # Dados de saneamento são liberados a cada ano
    # Assim, não é necessário calculá-los a cada vez que rodar o alerta
    try:
        resultados = spark.sql("""
            SELECT alrt_orgi_orga_dk, alrt_descricao, alrt_classe_hierarquia, alrt_key
            FROM {0}.{1}
            WHERE ano_referencia = {2}
        """.format(
                options['schema_exadata_aux'],
                options['isps_tabela_aux'],
                ano_referencia
            )
        )
        if resultados.count() > 0:
            return resultados
    except:
        pass

    agua = spark.sql("""
        WITH agregados AS (
            SELECT cod_mun, municipio, in009, in013, in023, in049
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_agua
            WHERE ano_referencia = {1}
            AND cod_prest IS NULL
        ),
        indicadores AS (
            SELECT municipio,
                CASE WHEN A.in009 < R.in009 THEN 'Índice de Hidrometação' ELSE NULL END AS ind1,
                CASE WHEN A.in013 > R.in013 THEN 'Índice de Perdas de Faturamento' ELSE NULL END AS ind2,
                CASE WHEN A.in023 < R.in023 THEN 'Índice de Atendimento Urbano de Água' ELSE NULL END AS ind3,
                CASE WHEN A.in049 > R.in049 THEN 'Índice de Perdas na Distribuição' ELSE NULL END AS ind4
            FROM agregados A
            JOIN (SELECT cod_mun, in009, in013, in023, in049 FROM agregados WHERE cod_mun = 33) R ON R.cod_mun != A.cod_mun
        )
        SELECT municipio, ind1 as alrt_descricao 
        FROM indicadores
        WHERE ind1 IS NOT NULL
        UNION ALL
        SELECT municipio, ind2 as alrt_descricao 
        FROM indicadores
        WHERE ind2 IS NOT NULL
        UNION ALL
        SELECT municipio, ind3 as alrt_descricao 
        FROM indicadores
        WHERE ind3 IS NOT NULL
        UNION ALL
        SELECT municipio, ind4 as alrt_descricao 
        FROM indicadores
        WHERE ind4 IS NOT NULL
    """.format(options["schema_opengeo"], ano_referencia))
    agua.createOrReplaceTempView('AGUA')


    esgoto = spark.sql("""
        WITH agregados AS (
            SELECT cod_mun, municipio, in015, in016, in024, in046
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_esgoto
            WHERE ano_referencia = {1}
            AND cod_prest IS NULL
        ),
        indicadores AS (
            SELECT municipio,
                CASE WHEN A.in015 < R.in015 THEN 'Índice de Coleta de Esgoto' ELSE NULL END AS ind1,
                CASE WHEN A.in016 < R.in016 THEN 'Índice de Tratamento de Esgoto' ELSE NULL END AS ind2,
                CASE WHEN A.in024 < R.in024 THEN 'Índice de Atendimento Urbano de Esgoto Referido' ELSE NULL END AS ind3,
                CASE WHEN A.in046 < R.in046 THEN 'Índice de Esgoto Tratado Referido à Água Consumida' ELSE NULL END AS ind4
            FROM agregados A
            JOIN (SELECT cod_mun, in015, in016, in024, in046 FROM agregados WHERE cod_mun = 33) R ON R.cod_mun != A.cod_mun
        )
        SELECT municipio, ind1 as alrt_descricao 
        FROM indicadores
        WHERE ind1 IS NOT NULL
        UNION ALL
        SELECT municipio, ind2 as alrt_descricao 
        FROM indicadores
        WHERE ind2 IS NOT NULL
        UNION ALL
        SELECT municipio, ind3 as alrt_descricao 
        FROM indicadores
        WHERE ind3 IS NOT NULL
        UNION ALL
        SELECT municipio, ind4 as alrt_descricao 
        FROM indicadores
        WHERE ind4 IS NOT NULL
    """.format(options["schema_opengeo"], ano_referencia))
    esgoto.createOrReplaceTempView('ESGOTO')

    # Tabela de drenagem não possui os agregados para o estado diretamente
    # Assim, é necessário calculá-los a partir dos dados de base
    drenagem = spark.sql("""
        WITH agregados AS (
            SELECT 
                sum(ri013)/sum(ge008) as in040,
                ((sum(ri029)+sum(ri067))/sum(ge006)) as in041,
                sum(ie024)/sum(ie017) as in021,
                sum(ie019)/sum(ie017) as in020
            FROM {0}.meio_ambiente_amb_saneamento_snis_drenagem_info_indic_2018
        ),
        indicadores AS (
            SELECT A.municipio,
                CASE WHEN A.in020 < R.in020 THEN 'Taxa de Cobertura de Pavimentação e Meio-Fio na Área Urbana do Município' ELSE NULL END AS ind1,
                CASE WHEN A.in021 > R.in021 THEN 'Taxa de Cobertura de Vias Públicas com Redes ou Canais Pluviais Subterrâneos na Área Urbana' ELSE NULL END AS ind2,
                CASE WHEN A.in040 > R.in040 THEN 'Parcela de Domicílios em Situação de Risco de Inundação' ELSE NULL END AS ind3,
                CASE WHEN A.in041 > R.in041 THEN 'Parcela da População Impactada por Eventos Hidrológicos' ELSE NULL END AS ind4
            FROM {0}.plataforma_amb_saneamento_snis_info_indic_drenagem A
            JOIN agregados R ON 1 = 1
            WHERE ano_referencia = {1}
        )
        SELECT municipio, ind1 as alrt_descricao 
        FROM indicadores
        WHERE ind1 IS NOT NULL
        UNION ALL
        SELECT municipio, ind2 as alrt_descricao 
        FROM indicadores
        WHERE ind2 IS NOT NULL
        UNION ALL
        SELECT municipio, ind3 as alrt_descricao 
        FROM indicadores
        WHERE ind3 IS NOT NULL
        UNION ALL
        SELECT municipio, ind4 as alrt_descricao 
        FROM indicadores
        WHERE ind4 IS NOT NULL
    """.format(options["schema_opengeo"], ano_referencia))
    drenagem.createOrReplaceTempView('DRENAGEM')

    INDICADORES = spark.sql("""
        SELECT * FROM AGUA
        UNION ALL
        SELECT * FROM ESGOTO
        UNION ALL
        SELECT * FROM DRENAGEM
    """)
    INDICADORES.createOrReplaceTempView('INDICADORES')

    resultados = spark.sql("""
        SELECT P.id_orgao as alrt_orgi_orga_dk, I.alrt_descricao, I.municipio as alrt_classe_hierarquia
        FROM {0}.atualizacao_pj_pacote P
        JOIN {1}.institucional_orgaos_meio_ambiente M ON M.cod_orgao = P.id_orgao
        JOIN INDICADORES I ON I.municipio = M.comarca
        WHERE cod_pct IN (20, 21, 22, 24, 28, 183)
    """.format(options['schema_exadata_aux'], options['schema_opengeo']))
    resultados.createOrReplaceTempView('RESULTADOS_ISPS')
    spark.catalog.cacheTable("RESULTADOS_ISPS")

    resultados = resultados.withColumn('ano_referencia', lit(ano_referencia).cast(IntegerType()))\
        .withColumn('alrt_key', uuidsha(*key_columns))

    resultados.write.mode('append').saveAsTable('{}.{}'.format(
        options['schema_exadata_aux'], options['isps_tabela_aux']
    ))

    return resultados.select(columns)
