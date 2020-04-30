from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from base import spark

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('docu_nr_externo').alias('alrt_docu_nr_externo'), 
    col('docu_tx_etiqueta').alias('alrt_docu_etiqueta'), 
    col('cldc_ds_classe').alias('alrt_docu_classe'),
    col('pcao_dt_andamento'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('cldc_ds_hierarquia').alias('alrt_classe_hierarquia'),
    col('elapsed'),
]

ciencias = [6374, 6375, 6376, 6377, 6378]
recursos = [
    6449, 6451, 6453, 6454, 6455, 6456, 6457, 6458, 6459, 6460, 6461, 6462,
    6463, 6464, 6465, 6466, 6467, 6468, 6470, 6471, 6472, 6473, 6474, 6475, 
    6476, 6477, 6478, 6479, 6529, 6530, 6554, 6555, 7824, 7825, 7850
]

def alerta_dt2i(options):
    documento = spark.table('%s.mcpr_documento' % options['schema_exadata'])
    classe = spark.table('%s.mmps_classe_hierarquia' % options['schema_exadata_aux'])
    vista = spark.table('%s.mcpr_vista' % options['schema_exadata'])
    andamento = spark.table('%s.mcpr_andamento' % options['schema_exadata'])
    sub_andamento = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata'])

    adt_ciencia = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).filter(col('stao_tppr_dk').isin(ciencias))
    adt_recurso = spark.table('%s.mcpr_sub_andamento' % options['schema_exadata']).filter(col('stao_tppr_dk').isin(recursos))

    doc_classe = documento.join(classe, documento.DOCU_CLDC_DK == classe.cldc_dk, 'left')
    doc_vista = doc_classe.join(vista, doc_classe.DOCU_DK == vista.VIST_DOCU_DK, 'inner')
    doc_andamento = doc_vista.join(andamento, doc_vista.VIST_DK == andamento.PCAO_VIST_DK, 'inner')
    doc_recente = doc_andamento.withColumn(
        'elapsed',
        lit(datediff(current_date(), 'pcao_dt_andamento')).cast(IntegerType())
    ).filter('elapsed <= 7').select(columns)
    
    doc_ciencia = doc_andamento.join(
        adt_ciencia,
        doc_andamento.PCAO_DK == adt_ciencia.STAO_PCAO_DK,
        'inner'
    ).select(
        col('docu_dk').alias('cie_docu_dk'),
        col('pcao_dt_andamento').alias('cie_dt_mov')
    )
    doc_recurso = doc_andamento.join(
        adt_recurso,
        doc_andamento.PCAO_DK == adt_recurso.STAO_PCAO_DK,
        'inner'
    ).select(
        col('docu_dk').alias('rec_docu_dk'),
        col('pcao_dt_andamento').alias('rec_dt_mov')
    )
    

    doc_cie_rec = doc_ciencia.join(
        doc_recurso,
        doc_ciencia.cie_docu_dk == doc_recurso.rec_docu_dk,
        'inner'
    ).filter('rec_dt_mov >= cie_dt_mov')
    doc_rec_week = doc_cie_rec.join(
        doc_recente,
        doc_cie_rec.rec_docu_dk == doc_recente.alrt_docu_dk,
        'inner' 
    )
    resultado = doc_rec_week.select(
        "alrt_docu_dk",
        "alrt_docu_nr_mp",
        "alrt_docu_nr_externo",
        "alrt_docu_etiqueta",
        "alrt_docu_classe",
        "alrt_orgi_orga_dk",
        "alrt_classe_hierarquia",
        "pcao_dt_andamento",
        "elapsed"
    ).groupby([
        "alrt_docu_dk",
        "alrt_docu_nr_mp",
        "alrt_docu_nr_externo",
        "alrt_docu_etiqueta",
        "alrt_docu_classe",
        "alrt_orgi_orga_dk",
        "alrt_classe_hierarquia"
    ]).agg(
        max("pcao_dt_andamento").alias("alrt_docu_date"),
        min("elapsed").alias("alrt_dias_passados") 
    )

    return resultado.select([
        'alrt_docu_dk', 
        'alrt_docu_nr_mp', 
        'alrt_docu_nr_externo', 
        'alrt_docu_etiqueta', 
        'alrt_docu_classe',
        'alrt_docu_date',  
        'alrt_orgi_orga_dk',
        'alrt_classe_hierarquia',
        'alrt_dias_passados',
    ])