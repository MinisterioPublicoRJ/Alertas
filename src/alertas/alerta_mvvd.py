#-*-coding:utf-8-*-
from pyspark.sql.types import IntegerType, StringType, TimestampType
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('alrt_key')
]

col_vict = [
    col('pesf_pess_dk').alias('vict_pess_dk'), 
    col('pesf_cpf').alias('vict_cpf'), 
    col('pesf_nr_rg').alias('vict_rg'), 
    col('pesf_nm_pessoa_fisica').alias('vict_nome'), 
    col('pesf_nm_mae').alias('vict_mae'),
    col('pesf_dt_nasc').alias('vict_nasc'),  
    col('docu_dk').alias('vict_docu_dk')
]

key_columns = [
    col('docu_dk')
]

def alerta_mvvd(options):
    pessoa = spark.table('%s.mcpr_pessoa_fisica' % options['schema_exadata'])
    pers_vitima = spark.table('%s.mcpr_personagem' % options['schema_exadata']).filter('pers_tppe_dk = 3 or pers_tppe_dk = 290')
    pessoa_vitima = pessoa.join(pers_vitima, pessoa.PESF_PESS_DK == pers_vitima.PERS_PESS_DK, 'inner')
    
    doc_agressao = spark.sql("from documento")\
        .filter('docu_mate_dk = 43')
    vitimas_passadas = pessoa_vitima\
        .join(doc_agressao, pessoa_vitima.PERS_DOCU_DK == doc_agressao.DOCU_DK, 'inner')\
        .select(col_vict)
 
    documento = spark.sql("from documento")\
        .filter(datediff(current_date(), 'docu_dt_cadastro') <= 30)\
        .filter('docu_mate_dk = 43')
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_vitima = pessoa_vitima.join(doc_classe, pessoa_vitima.PERS_DOCU_DK == doc_classe.DOCU_DK, 'inner')

    vitimas_passadas.registerTempTable('vitimas_passadas')
    doc_vitima.registerTempTable('doc_vitima')
    resultado = spark.sql("""
        SELECT * 
        FROM doc_vitima d JOIN vitimas_passadas v ON d.pesf_pess_dk = v.vict_pess_dk AND v.vict_docu_dk != d.docu_dk
        UNION ALL
        SELECT * 
        FROM doc_vitima d JOIN vitimas_passadas v ON d.pesf_cpf = v.vict_cpf AND v.vict_docu_dk != d.docu_dk
        WHERE d.pesf_cpf != '00000000000'
        UNION ALL
        SELECT * 
        FROM doc_vitima d JOIN vitimas_passadas v ON d.pesf_nr_rg = v.vict_rg AND v.vict_docu_dk != d.docu_dk
        UNION ALL
        SELECT * 
        FROM doc_vitima d JOIN vitimas_passadas v ON d.pesf_nm_pessoa_fisica = v.vict_nome AND d.pesf_nm_mae = v.vict_mae AND v.vict_docu_dk != d.docu_dk
        UNION ALL
        SELECT * 
        FROM doc_vitima d JOIN vitimas_passadas v ON d.pesf_nm_pessoa_fisica = v.vict_nome AND d.pesf_dt_nasc = v.vict_nasc AND v.vict_docu_dk != d.docu_dk
    """)

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns).distinct()
