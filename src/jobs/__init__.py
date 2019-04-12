# -*- coding: utf-8 -*- 
import sys
sys.path.append("..")

from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, TimestampType

from shared.timer import Timer
from ouvi import alerta_ouvi

class Alertas:

    alerta_list = {
        'OUVI': 'Expedientes Ouvidoria (EO) pendentes de recebimento',
    }

    @staticmethod
    def now():
        return datetime.today().strftime('%Y-%m-%d')

    @classmethod
    def generateAlertas(cls):
        print('Gerando alertas')
        with Timer():
            for alerta in cls.alerta_list:
                cls.generateAlerta(alerta)

    @classmethod
    def generateAlerta(cls, alerta):
        print('Gerando alertas do tipo: {0}'.format(cls.alerta_list[alerta]))
        dataframe = None
        with Timer():
            if alerta == 'OUVI':
                dataframe = alerta_ouvi().\
                    withColumn('alrt_dias_passados', lit('-1').cast(IntegerType())).\
                    withColumn('alrt_descricao', lit(cls.alerta_list[alerta]).cast(StringType())).\
                    withColumn('alrt_sigla', lit(alerta).cast(StringType())).\
                    withColumn('alrt_found', lit(Alertas.now()).cast(TimestampType()))
            else:
                raise KeyError('Alerta desconhecido')
        
        print('Salvando alertas do tipo {0}'.format(cls.alerta_list[alerta]))
        if dataframe:
            with Timer():
                dataframe.write.format('hive').\
                    saveAsTable('exadata_aux.mmps_alertas', mode='append')
        else:
            raise ValueError('Alerta nao gerado')
