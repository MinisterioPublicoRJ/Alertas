#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


columns = [
    col('id_orgao').alias('alrt_orgi_orga_dk'),
    col('contratacao').alias('comp_contratacao'),
    col('item').alias('comp_item'),
    col('id_item').alias('comp_id_item'),
    col('contrato_iditem').alias('comp_contrato_iditem'),
    col('dt_contratacao').alias('comp_dt_contratacao'),
    col('var_perc').alias('comp_var_perc'),
    col('alrt_key'),
]

key_columns = [
    col('contrato_iditem')
]

def alerta_comp(options):
    df1 = spark.sql("""
        SELECT contratacao, id_item, contrato_iditem, item, dt_contratacao, var_perc
        FROM {0}.compras_fora_padrao_capital
        WHERE var_perc >= 20
    """.format(options["schema_alertas_compras"]))

    df2 = spark.sql("""
        SELECT id_orgao
        FROM {0}.atualizacao_pj_pacote
        WHERE UPPER(pacote_atribuicao) LIKE '%%CIDADANIA%%' AND orgao_codamp LIKE '%%CAPITAL%%'
    """.format(options["schema_exadata_aux"]))

    df = df1.crossJoin(df2)
    df = df.withColumn('alrt_key', uuidsha(*key_columns))

    return df.select(columns)
