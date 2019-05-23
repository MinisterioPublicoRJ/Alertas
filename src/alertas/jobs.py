# -*- coding: utf-8 -*- 
import sys
sys.path.append("..")

import uuid

from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from base import spark
from timer import Timer
from alerta_dord import alerta_dord
from alerta_mvvd import alerta_mvvd
from alerta_ouvi import alerta_ouvi
from alerta_ppfp import alerta_ppfp
from alerta_vadf import alerta_vadf

class AlertaSession:
    alerta_list = {
        'DORD': 'Documentos com Órgão Responsável possivelmente desatualizado',
        'MVVD': 'Documentos com vitimas recorrentes recebidos nos ultimos 30 dias',
        'OUVI': 'Expedientes de Ouvidoria (EO) pendentes de recebimento',
        'PPFP': 'Procedimento Preparatório fora do prazo',
        'VADF': 'Vistas abertas em documentos já fechados',
    }
    STATUS_RUNNING = "RUNNING"
    STATUS_FINISHED = "FINISHED"
    STATUS_ERROR = "ERROR"

    def __init__(self):
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
        session_df.write.format('hive').saveAsTable('exadata_aux.mmps_alerta_sessao', mode='append')
            
    def generateAlertas(self):
        print('Verificando alertas existentes em {0}'.format(datetime.today()))
        with Timer():
            for alerta in self.alerta_list:
                self.generateAlerta(alerta)
            self.wrapAlertas()

    def generateAlerta(self, alerta):
        print('Verificando alertas do tipo: {0}'.format(self.alerta_list[alerta]))
        dataframe = None
        with Timer():
            if alerta == 'OUVI':
                dataframe = alerta_ouvi().\
                    withColumn('alrt_dias_passados', lit('-1').cast(IntegerType())).\
                    withColumn('alrt_descricao', lit(self.alerta_list[alerta]).cast(StringType())).\
                    withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                    withColumn('alrt_session', lit(self.session_id).cast(StringType()))
            elif alerta == 'DORD':
                dataframe = alerta_dord().\
                    withColumn('alrt_dias_passados', lit('-1').cast(IntegerType())).\
                    withColumn('alrt_descricao', lit(self.alerta_list[alerta]).cast(StringType())).\
                    withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                    withColumn('alrt_session', lit(self.session_id).cast(StringType()))
            elif alerta == 'MVVD':
                dataframe = alerta_dord().\
                    withColumn('alrt_dias_passados', lit('-1').cast(IntegerType())).\
                    withColumn('alrt_descricao', lit(self.alerta_list[alerta]).cast(StringType())).\
                    withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                    withColumn('alrt_session', lit(self.session_id).cast(StringType()))
            elif alerta == 'VADF':
                dataframe = alerta_vadf().\
                    withColumn('alrt_dias_passados', lit('-1').cast(IntegerType())).\
                    withColumn('alrt_descricao', lit(self.alerta_list[alerta]).cast(StringType())).\
                    withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                    withColumn('alrt_session', lit(self.session_id).cast(StringType()))
            elif alerta == 'PPFP':
                dataframe = alerta_ppfp().\
                    withColumn('alrt_descricao', lit(self.alerta_list[alerta]).cast(StringType())).\
                    withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                    withColumn('alrt_session', lit(self.session_id).cast(StringType()))
            else:
                raise KeyError('Alerta desconhecido')
        
        print('Gravando alertas do tipo {0}'.format(self.alerta_list[alerta]))
        if dataframe:
            with Timer():
                dataframe.write.format('hive').\
                    saveAsTable('exadata_aux.mmps_alertas', mode='append')
        else:
            self.status = self.STATUS_ERROR
            raise ValueError('Alerta nao gerado')


