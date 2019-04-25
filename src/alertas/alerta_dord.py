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

def alerta_dord():
    documento = spark.table('exadata.mcpr_documento')
    classe = spark.table('exadata_aux.mmps_classe_hierarquia')
    vista = spark.table('exadata_aux.mcpr_vista')
    andamento = spark.table('exadata_aux.mcpr_andamento')
    doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.docu_dk == vista.vist_docu_dk, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.vist_dk == andamento.pcao_vist_dk, 'inner')

    # TODO Implementar o trecho
    # and a.pcao_dk = (                                 
	#		SELECT MAX(z.pcao_dk)                         
	#		FROM                                                    
	#			mcpr_vista x                                        
	#			JOIN mcpr_andamento z ON x.vist_dk = z.pcao_vist_dk
	#			JOIN orgi_orgao ok ON x.VIST_ORGI_ORGA_DK = ok.ORGI_CDORGAO
	#		WHERE 
	#			x.vist_docu_dk = d.docu_dk                        
	#			AND o.ORGI_TPOR_DK = 1
	#	)
    return doc_andamento.\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('vist_orgi_orga_dk != docu_orgi_orga_dk_responsavel').\
        filter('pcao_tpsa_dk = 2').\
        select(columns)
