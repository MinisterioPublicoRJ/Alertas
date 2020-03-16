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
from alerta_ic1a import alerta_ic1a
from alerta_mvvd import alerta_mvvd
from alerta_nf30 import alerta_nf30
from alerta_offp import alerta_offp
from alerta_ouvi import alerta_ouvi
from alerta_pa1a import alerta_pa1a
from alerta_ppfp import alerta_ppfp
from alerta_vadf import alerta_vadf


class AlertaSession:
    alerta_list = {
        'DCTJ': ['Documentos criminais sem retorno do TJ a mais de 60 dias', alerta_dctj],
        'DNTJ': ['Documentos não criminais sem retorno do TJ a mais de 120 dias', alerta_dntj],
        # 'DORD': ['Documentos com Órgão Responsável possivelmente desatualizado', alerta_dord],
        'IC1A': ['ICs sem prorrogação por mais de um ano', alerta_ic1a],
        'MVVD': ['Documentos com vitimas recorrentes recebidos nos ultimos 30 dias', alerta_mvvd],
        'OFFP': ['Ofício fora do prazo', alerta_offp],
        'OUVI': ['Expedientes de Ouvidoria (EO) pendentes de recebimento', alerta_ouvi],
        'PA1A': ['Procedimento Preparatório fora do prazo', alerta_pa1a],
        'PPFP': ['PAs sem prorrogação por mais de um ano', alerta_ppfp],
        'VADF': ['Vistas abertas em documentos já fechados', alerta_vadf],
        #'NF30': 'Notícia de Fato a mais de 120 dias',
    }
    STATUS_RUNNING = "RUNNING"
    STATUS_FINISHED = "FINISHED"
    STATUS_ERROR = "ERROR"

    def __init__(self, options):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
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
        session_df.coalesce(1).write.format('parquet').saveAsTable('%s.mmps_alerta_sessao' % self.options['schema_exadata_aux'], mode='append')
            
    def generateAlertas(self):
        print('Verificando alertas existentes em {0}'.format(datetime.today()))
        with Timer():
            dfs = []
            for alerta, (desc, func) in self.alerta_list.items():
                dfs.append(self.generateAlerta(alerta, desc, func))
            df = reduce(DataFrame.unionAll, dfs)
            self.write_dataframe(df)
            self.wrapAlertas()

    def generateAlerta(self, alerta, desc, func):
        print('Verificando alertas do tipo: {0}'.format(alerta))
        with Timer():
            dataframe = func(self.options)
            dataframe = dataframe.withColumn('alrt_dias_passados', lit(-1)) if 'alrt_dias_passados' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_descricao', lit(desc).cast(StringType())).\
                withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                withColumn('alrt_session', lit(self.session_id).cast(StringType()))
        return dataframe
    
    def check_table_exists(self, schema, table_name):
        spark.sql("use %s" % schema)
        result_table_check = spark.sql("SHOW TABLES LIKE '%s'" % table_name).count()
        return True if result_table_check > 0 else False

    def write_dataframe(self, dataframe):
        #print('Gravando alertas do tipo {0}'.format(self.alerta_list[alerta]))
        with Timer():
            dataframe = dataframe.withColumn("dt_partition", date_format(current_timestamp(), "ddMMyyyy"))
            
            is_exists_table_alertas = self.check_table_exists(self.options['schema_exadata_aux'], "mmps_alertas")
            table_name = '%s.mmps_alertas' % self.options['schema_exadata_aux']
            if is_exists_table_alertas:
                dataframe.coalesce(20).write.mode("overwrite").insertInto(table_name, overwrite=True)
            else:
                dataframe.write.partitionBy("dt_partition").saveAsTable(table_name)

            _update_impala_table(table_name, self.options['impala_host'], self.options['impala_port'])


