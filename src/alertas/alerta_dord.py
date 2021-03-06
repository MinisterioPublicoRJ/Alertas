#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('pcao_dt_andamento').alias('alrt_date_referencia'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
]

def alerta_dord(options):
    #documento = spark.table('%s.mcpr_documento' % options['schema_exadata'])
    documento = spark.sql("from documento")
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    # vista = spark.table('%s.mcpr_vista' % options['schema_exadata'])
    vista = spark.sql("from vista")
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata']).filter('pcao_tpsa_dk = 2')
   
    doc_classe = documento.join(broadcast(classe), documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_vista = doc_classe.join(vista, vista.VIST_DOCU_DK == documento.DOCU_DK)
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK)
    last_andamento = doc_andamento.select(['docu_dk', 'pcao_dt_andamento']).\
        groupBy('docu_dk').agg({'pcao_dt_andamento': 'max'}).\
        withColumnRenamed('max(pcao_dt_andamento)', 'last_date').\
        withColumnRenamed('docu_dk', 'land_docu_dk')
    check_andamento = doc_andamento.join(
        last_andamento, 
        (doc_andamento.DOCU_DK == last_andamento.land_docu_dk) & (doc_andamento.PCAO_DT_ANDAMENTO == last_andamento.last_date)
    )

    return check_andamento.\
        filter('docu_tpst_dk != 11').\
        filter('docu_fsdc_dk = 1').\
        filter('vist_orgi_orga_dk != docu_orgi_orga_dk_responsavel').\
        select(columns)
