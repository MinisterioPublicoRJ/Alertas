# -*- coding: utf-8 -*- 
import sys
sys.path.append("..")

import uuid

from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from base import spark
from pyspark.sql import DataFrame
from timer import Timer
from utils import _update_impala_table
from alerta_dctj import alerta_dctj
from alerta_dntj import alerta_dntj
from alerta_dord import alerta_dord
from alerta_dt2i import alerta_dt2i
from alerta_gate import alerta_gate
from alerta_ic1a import alerta_ic1a
from alerta_mvvd import alerta_mvvd
from alerta_nf30 import alerta_nf30
from alerta_offp import alerta_offp
from alerta_ouvi import alerta_ouvi
from alerta_pa1a import alerta_pa1a
from alerta_ppfp import alerta_ppfp
from alerta_prcr import alerta_prcr
from alerta_vadf import alerta_vadf


class AlertaSession:
    alerta_list = {
        # 'DCTJ': ['Documentos criminais sem retorno do TJ a mais de 60 dias', alerta_dctj, None],
        # 'DNTJ': ['Documentos não criminais sem retorno do TJ a mais de 120 dias', alerta_dntj, None],
        # 'DORD': ['Documentos com Órgão Responsável possivelmente desatualizado', alerta_dord], None,
        'GATE': ['Documentos com novas ITs do GATE', alerta_gate, 'MMPS_ALERTA_GATE'],
        'IC1A': ['ICs sem prorrogação por mais de um ano', alerta_ic1a, None],
        'MVVD': ['Documentos com vitimas recorrentes recebidos nos ultimos 30 dias', alerta_mvvd, None],
        'OFFP': ['Ofício fora do prazo', alerta_offp, None],
        'OUVI': ['Expedientes de Ouvidoria (EO) pendentes de recebimento', alerta_ouvi, None],
        'PA1A': ['PAs sem prorrogação por mais de um ano', alerta_pa1a, None],
        'PPFP': ['Procedimento Preparatório fora do prazo', alerta_ppfp, None],
        'PRCR': ['Processo possivelmente prescrito', alerta_prcr, None],
        'VADF': ['Vistas abertas em documentos já fechados', alerta_vadf, None],
        'NF30': ['Notícia de Fato a mais de 120 dias', alerta_nf30, None],
        'DT2I': ['Movimento em processo de segunda instância', alerta_dt2i, None],
    }
    STATUS_RUNNING = "RUNNING"
    STATUS_FINISHED = "FINISHED"
    STATUS_ERROR = "ERROR"

    TEMP_TABLE_NAME = "teste_temp_mmps_alertas"
    FINAL_TABLE_NAME = "teste_mmps_alertas"
    SESSION_TABLE_NAME = "teste_mmps_alerta_sessao"

    def __init__(self, options):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        self.options = options
        self.session_id = str(uuid.uuid4().int & (1<<60)-1)
        self.start_session = self.now()
        self.end_session = None
        self.status = self.STATUS_RUNNING

    @staticmethod
    def now():
        return datetime.now()
        
    def wrapAlertas(self):
        fields = [
            StructField("ALRT_SESSION_DK", StringType(), True),
            StructField("ALRT_SESSION_START", TimestampType(), True),
            StructField("ALRT_SESSION_FINISH", TimestampType(), True),
            StructField("ALRT_SESSION_STATUS", StringType(), True)
        ]
        schema = StructType(fields)

        self.end_session = self.now()
        if self.status == self.STATUS_RUNNING:
            self.status = self.STATUS_FINISHED
        
        data = [(self.session_id, self.start_session, self.end_session, self.status)]
        
        session_df = spark.createDataFrame(data, schema)
        session_df = session_df.withColumn("dt_partition", date_format(current_timestamp(), "ddMMyyyy"))
        session_df.coalesce(1).write.format('parquet').saveAsTable(
            '{0}.{1}'.format(self.options['schema_exadata_aux'], self.SESSION_TABLE_NAME),
            mode='append')
            
    def generateAlertas(self):
        print('Verificando alertas existentes em {0}'.format(datetime.today()))
        with Timer():
            spark.table('%s.mcpr_documento' % self.options['schema_exadata']) \
                .createOrReplaceTempView("documento")
            spark.catalog.cacheTable("documento")
            spark.sql("from documento").count()

            spark.table('%s.mcpr_vista' % self.options['schema_exadata']) \
                .createOrReplaceTempView("vista")
            spark.catalog.cacheTable("vista")
            spark.sql("from vista").count()

            for alerta, (desc, func, custom_table) in self.alerta_list.items():
                self.generateAlerta(alerta, desc, func)
            self.write_dataframe(dataframe=custom_table)
            self.wrapAlertas()

    def generateAlerta(self, alerta, desc, func):
        print('Verificando alertas do tipo: {0}'.format(alerta))
        with Timer():
            dataframe = func(self.options)
            dataframe = dataframe.withColumn('alrt_dk', lit('NO_ID')) if 'alrt_dk' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_dias_passados', lit(-1)) if 'alrt_dias_passados' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_descricao', lit(desc).cast(StringType())).\
                withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                withColumn('alrt_session', lit(self.session_id).cast(StringType())).\
                withColumn("dt_partition", date_format(current_timestamp(), "ddMMyyyy"))

            dataframe.write.mode("append").saveAsTable(self.TEMP_TABLE_NAME)

    def check_table_exists(self, schema, table_name):
        spark.sql("use %s" % schema)
        result_table_check = spark.sql("SHOW TABLES LIKE '%s'" % table_name).count()
        return True if result_table_check > 0 else False

    def write_dataframe(self, dataframe=None):
        #print('Gravando alertas do tipo {0}'.format(self.alerta_list[alerta]))
        with Timer():
            table = dataframe if dataframe else self.FINAL_TABLE_NAME
            temp_table_df = spark.table(self.TEMP_TABLE_NAME)

            is_exists_table_alertas = self.check_table_exists(self.options['schema_exadata_aux'], table)
            table_name = '{0}.{1}'.format(self.options['schema_exadata_aux'], table)
            if is_exists_table_alertas:
                temp_table_df.repartition("dt_partition").write.mode("overwrite").insertInto(table_name, overwrite=True)
            else:
                temp_table_df.repartition("dt_partition").write.partitionBy("dt_partition").saveAsTable(table_name)
            
            spark.sql("drop table default.{0}".format(self.TEMP_TABLE_NAME))

            _update_impala_table(table_name, self.options['impala_host'], self.options['impala_port'])


