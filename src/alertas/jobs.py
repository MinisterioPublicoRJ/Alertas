# -*- coding: utf-8 -*- 
import sys
sys.path.append("..")

import uuid

from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import (
    StringType,
    IntegerType,
    TimestampType,
    DoubleType,
    StructField,
    StructType
)
from pyspark.sql.utils import AnalysisException

from base import spark
from pyspark.sql import DataFrame
from timer import Timer
from alerta_bdpa import alerta_bdpa
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
from alerta_ro import alerta_ro
from alerta_vadf import alerta_vadf
from alerta_abr1 import alerta_abr1
from alerta_isps import alerta_isps
from alerta_comp import alerta_comp


class AlertaSession:
    # Table Names
    TYPES_TABLE_NAME = 'mmps_alertas_tipos'

    ABR1_TABLE_NAME = 'mmps_alertas_abr1'
    RO_TABLE_NAME = 'mmps_alertas_ro'
    COMP_TABLE_NAME = 'mmps_alertas_comp'
    ISPS_TABLE_NAME = 'mmps_alertas_isps'
    MGP_TABLE_NAME = 'mmps_alertas_mgp'

    PRCR_DETALHE_TABLE_NAME = "mmps_alerta_detalhe_prcr"
    ISPS_AUX_TABLE_NAME = "mmps_alerta_isps_aux"

    # Ordem em que as colunas estão salvas na tabela final
    # Esta ordem deve ser mantida por conta do insertInto que é realizado
    COLUMN_ORDER_BASE = [
        ('alrt_key', StringType),
        ('alrt_sigla', StringType),
        ('alrt_orgi_orga_dk', IntegerType)
    ]
    COLUMN_ORDER_ABR1 = COLUMN_ORDER_BASE + [
        ('abr1_nr_procedimentos', IntegerType),
        ('abr1_ano_mes', StringType)
    ]
    COLUMN_ORDER_RO = COLUMN_ORDER_BASE + [
        ('ro_nr_delegacia', StringType),
        ('ro_qt_ros_faltantes', IntegerType),
        ('ro_max_proc', StringType)
    ]
    COLUMN_ORDER_COMP = COLUMN_ORDER_BASE + [
        ('comp_contratacao', IntegerType),
        ('comp_item', StringType),
        ('comp_id_item', IntegerType),
        ('comp_contrato_iditem', StringType),
        ('comp_dt_contratacao', StringType),
        ('comp_var_perc', DoubleType)
    ]
    COLUMN_ORDER_ISPS = COLUMN_ORDER_BASE + [
        ('isps_municipio', StringType),
        ('isps_indicador', StringType),
        ('isps_ano_referencia', IntegerType)
    ]
    COLUMN_ORDER_MGP = COLUMN_ORDER_BASE + [
        ('alrt_docu_dk', IntegerType),
        ('alrt_docu_nr_mp', StringType),
        ('alrt_date_referencia', TimestampType),
        ('alrt_dias_referencia', IntegerType),
        ('alrt_dk_referencia', IntegerType),
        ('alrt_info_adicional', StringType)
    ]

    alerta_list = {
        # 'DCTJ': [alerta_dctj],
        # 'DNTJ': [alerta_dntj],
        # 'DORD': [alerta_dord],
        'GATE': [alerta_gate, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'BDPA': [alerta_bdpa, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'IC1A': [alerta_ic1a, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'MVVD': [alerta_mvvd, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        # 'OFFP': [alerta_offp],
        'OUVI': [alerta_ouvi, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'PA1A': [alerta_pa1a, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'PPFP': [alerta_ppfp, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'PRCR': [alerta_prcr, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'VADF': [alerta_vadf, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'NF30': [alerta_nf30, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'DT2I': [alerta_dt2i, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'RO': [alerta_ro, RO_TABLE_NAME, COLUMN_ORDER_RO],
        'ABR1': [alerta_abr1, ABR1_TABLE_NAME, COLUMN_ORDER_ABR1],
        'ISPS': [alerta_isps, ISPS_TABLE_NAME, COLUMN_ORDER_ISPS],
        'COMP': [alerta_comp, COMP_TABLE_NAME, COLUMN_ORDER_COMP],
    }

    TABLE_NAMES = set(x[1] for x in alerta_list.values())

    def __init__(self, options):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        self.options = options
        # Setando o nome das tabelas de detalhe aqui, podemos centralizá-las como atributos de AlertaSession
        self.options['prescricao_tabela_detalhe'] = self.PRCR_DETALHE_TABLE_NAME
        self.options['isps_tabela_aux'] = self.ISPS_AUX_TABLE_NAME

        self.hist_name = lambda x: 'hist_' + x

        # Definir o schema no nome da tabela temp evita possíveis conflitos
        # entre processos em produção e desenvolvimento
        self.temp_name = lambda x: '{0}.temp_{1}'.format(options['schema_alertas'], x)

        # Evita que tabela temporária de processos anteriores com erro
        # influencie no resultado do processo atual.
        for table in self.TABLE_NAMES:
            spark.sql("DROP TABLE IF EXISTS {0}".format(self.temp_name(table)))

    @staticmethod
    def now():
        return datetime.now()

    def generateTypesTable(self):
        alert_types = [
            ('DCTJ', 'Documentos criminais sem retorno do TJ a mais de 60 dias'),
            ('DNTJ', 'Documentos não criminais sem retorno do TJ a mais de 120 dias'),
            ('DORD', 'Documentos com Órgão Responsável possivelmente desatualizado'),
            ('GATE', 'Documentos com novas ITs do GATE'),
            ('BDPA', 'Baixas a DP em atraso'),
            ('IC1A', 'ICs sem prorrogação por mais de um ano'),
            ('MVVD', 'Documentos com vitimas recorrentes recebidos nos ultimos 30 dias'),
            ('OFFP', 'Ofício fora do prazo'),
            ('OUVI', 'Expedientes de Ouvidoria (EO) pendentes de recebimento'),
            ('PA1A', 'PAs sem prorrogação por mais de um ano'),
            ('PPFP', 'Procedimento Preparatório fora do prazo'),
            ('PPPV', 'Procedimento Preparatório próximo de vencer'),
            ('PRCR', 'Processo possivelmente prescrito'),
            ('PRCR1', 'Todos os crimes prescritos'),
            ('PRCR2', 'Todos os crimes próximos de prescrever'),
            ('PRCR3', 'Algum crime prescrito'),
            ('PRCR4', 'Algum crime próximo de prescrever'),
            ('VADF', 'Vistas abertas em documentos já fechados'),
            ('NF30', 'Notícia de Fato a mais de 120 dias'),
            ('DT2I', 'Movimento em processo de segunda instância'),
            ('RO', 'ROs não entregues pelas delegacias'),
            ('ABR1', 'Procedimentos que têm mais de 1 ano para comunicar ao CSMP'),
            ('ISPS', 'Indicadores de Saneamento em Vermelho'),
            ('COMP', 'Compras fora do padrão'),
        ]

        fields = [
            StructField("alrt_sigla", StringType(), False),
            StructField("alrt_descricao", StringType(), False),
        ]
        schema = StructType(fields)

        df = spark.createDataFrame(alert_types, schema)
        df.coalesce(1).write.format('parquet').saveAsTable(
            '{0}.{1}'.format(self.options['schema_alertas'], self.TYPES_TABLE_NAME),
            mode='overwrite')
    
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

            for alerta, (func, table, columns) in self.alerta_list.items():
                self.generateAlerta(alerta, func, table, columns)
            self.write_dataframe()
            # self.generateTypesTable()

    def generateAlerta(self, alerta, func, table, columns):
        print('Verificando alertas do tipo: {0}'.format(alerta))
        with Timer():
            dataframe = func(self.options)
            dataframe = dataframe.withColumn('alrt_sigla', lit(alerta).cast(StringType())) if 'alrt_sigla' not in dataframe.columns else dataframe

            # A chave DEVE ser definida dentro do alerta, senão a funcionalidade de dispensa pode não funcionar
            # formato sigla.chave.orgao
            dataframe = dataframe.withColumn('alrt_key', concat(
                col('alrt_sigla'), lit('.'),
                col('alrt_key') if 'alrt_key' in dataframe.columns else lit('KEYUNDEFINED'),
                lit('.'), col('alrt_orgi_orga_dk')
                )
            )

            for colname, coltype in columns:
                dataframe = dataframe.withColumn(colname, lit(None).cast(coltype())) if colname not in dataframe.columns else dataframe

            colnames = [c[0] for c in columns]
            dataframe.select(colnames).write.mode("append").saveAsTable(self.temp_name(table))

    def check_table_exists(self, schema, table_name):
        spark.sql("use %s" % schema)
        result_table_check = spark.sql("SHOW TABLES LIKE '%s'" % table_name).count()
        return True if result_table_check > 0 else False

    def write_dataframe(self):
        with Timer():
            for table in self.TABLE_NAMES:
                print("Escrevendo a tabela {}".format(table))
                temp_table_df = spark.table(self.temp_name(table))

                table_name = '{0}.{1}'.format(self.options['schema_alertas'], table)
                temp_table_df.repartition(3).write.mode("overwrite").saveAsTable(table_name)

                hist_table_df = temp_table_df.\
                    withColumn("dt_calculo", date_format(current_timestamp(), "yyyyMMdd")).\
                    withColumn("dt_partition", date_format(current_timestamp(), "yyyyMM"))
                hist_table_name = '{0}.{1}'.format(self.options['schema_alertas'], self.hist_name(table))
                try:
                    current_hist = spark.sql("""
                        SELECT * FROM {0} WHERE dt_partition = '{1}' AND dt_calculo <> '{2}'
                    """.format(
                            hist_table_name,
                            datetime.now().strftime('%Y%m'),
                            datetime.now().strftime('%Y%m%d')
                        )
                    )
                except:
                    current_hist = None

                if current_hist:
                    hist_table_df = current_hist.union(hist_table_df)
                    hist_table_df.write.saveAsTable(hist_table_name + "_temp")
                    hist_table_df = spark.table(hist_table_name + "_temp")
                    hist_table_df.coalesce(3).write.mode("overwrite").insertInto(hist_table_name, overwrite=True)
                    spark.sql("drop table {0}".format(hist_table_name + "_temp"))
                else:
                    hist_table_df.coalesce(3).write.partitionBy("dt_partition").saveAsTable(hist_table_name)
    
                spark.sql("drop table {0}".format(self.temp_name(table)))
