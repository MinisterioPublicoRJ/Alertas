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

col_vict = [
    col('pesf_pess_dk').alias('vict_pess_dk'), 
    col('pesf_cpf').alias('vict_cpf'), 
    col('pesf_nr_rg').alias('vict_rg'), 
    col('pesf_nm_pessoa_fisica').alias('vict_nome'), 
    col('pesf_nm_mae').alias('vict_mae'),
    col('pesf_dt_nasc').alias('vict_nasc'),  
    col('docu_dk').alias('vict_docu_dk')
]

def alerta_mvvd():
    pessoa = spark.table('exadata.mcpr_pessoa_fisica')
    pers_vitima = spark.table('exadata.mcpr_personagem').filter('pers_tppe_dk = 3 or pers_tppe_dk = 290')
    pessoa_vitima = pessoa.join(pers_vitima, pessoa.pesf_pess_dk == pers_vitima.pers_pess_dk, 'inner')
    
    doc_agressao = spark.table('exadata.mcpr_documento').filter('docu_mate_dk = 43')
    vitimas_passadas = pessoa_vitima\
        .join(doc_agressao, pessoa_vitima.pers_docu_dk == doc_agressao.docu_dk, 'inner')\
        .select(col_vict)
 
    documento = spark.table('exadata.mcpr_documento')\
        .filter(datediff(current_date(), 'docu_dt_cadastro') <= 30)\
        .filter('docu_mate_dk = 43')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_vitima = pessoa_vitima.join(doc_classe, pessoa_vitima.pers_docu_dk == doc_classe.docu_dk, 'inner')

    vitimas_passadas.registerTempTable('vitimas_passadas')
    doc_vitima.registerTempTable('doc_vitima')
    resultado = spark.sql("""SELECT * 
        FROM doc_vitima d
            JOIN vitimas_passadas v ON (
                d.pesf_pess_dk = v.vict_pess_dk
                OR d.pesf_cpf = v.vict_cpf
                OR d.pesf_nr_rg = v.vict_rg
                OR (d.pesf_nm_pessoa_fisica = v.vict_nome AND(
                    d.pesf_nm_mae = v.vict_mae
                    OR d.pesf_dt_nasc = v.vict_nasc
                ))
            )""")

    return resultado.select(columns)
