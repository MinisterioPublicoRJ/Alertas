import pyspark
from timer import Timer

with Timer():
    print('Creating Spark Session')
    spark = pyspark.sql.session.SparkSession\
        .builder\
        .appName("alertas_dominio")\
        .enableHiveSupport()\
        .getOrCreate()

    sc = spark.sparkContext

    sc.setCheckpointDir('hdfs:///user/ebarbara/temp')
