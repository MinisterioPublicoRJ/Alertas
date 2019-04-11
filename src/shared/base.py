import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, DateType

from timer import Timer

from datetime import datetime


def today():
    return datetime.today().strftime('%Y-%m-%d')


def make_alerta(dataframe, alerta=None):
    if alerta == 'OUVI':
        return dataframe.\
            withColumn('alrt_dias_passados', lit('-1').cast(IntegerType())).\
            withColumn('alrt_descricao', lit('Expedientes Ouvidoria (EO) pendentes de recebimento').cast(StringType())).\
            withColumn('alrt_sigla', lit('OUVI').cast(StringType())).\
            withColumn('alrt_found', lit(today()).cast(DateType()))
    else:
        return dataframe


def save_alerta(dataframe):
    dataframe.write.format('hive').\
        saveAsTable('exadata_aux.mmps_alertas', mode='append')

with Timer():
    print('Creating Spark Session')
    spark = pyspark.sql.session.SparkSession\
        .builder\
        .appName("alertas_dominio")\
        .enableHiveSupport()\
        .getOrCreate()

    sc = spark.sparkContext

    sc.setCheckpointDir('hdfs:///user/ebarbara/temp')
