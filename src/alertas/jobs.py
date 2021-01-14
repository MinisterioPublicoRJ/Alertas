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


class AlertaSession:
    # Table Names
    TYPES_TABLE_NAME = 'mmps_alertas_tipos'

    ABR1_TABLE_NAME = 'mmps_alertas_abr1'
    RO_TABLE_NAME = 'mmps_alertas_ro'
    COMP_TABLE_NAME = 'mmps_alertas_comp'
    ISPS_TABLE_NAME = 'mmps_alertas_isps'
    MGP_TABLE_NAME = 'mmps_alertas_mgp'
    PPFP_TABLE_NAME = 'mmps_alertas_ppfp'
    GATE_TABLE_NAME = 'mmps_alertas_gate'
    VADF_TABLE_NAME = 'mmps_alertas_vadf'
    OUVI_TABLE_NAME = 'mmps_alertas_ouvi'

    PRCR_DETALHE_TABLE_NAME = "testkey_mmps_alerta_detalhe_prcr"
    ISPS_AUX_TABLE_NAME = "testkey_mmps_alerta_isps_aux"

    # Ordem em que as colunas estão salvas na tabela final
    # Esta ordem deve ser mantida por conta do insertInto que é realizado
    COLUMN_ORDER_BASE = ['alrt_key', 'alrt_sigla', 'alrt_orgi_orga_dk']
    COLUMN_ORDER_ABR1 = COLUMN_ORDER_BASE + ['abr1_nr_procedimentos', 'abr1_ano_mes']
    COLUMN_ORDER_RO = COLUMN_ORDER_BASE + [
        'ro_nr_delegacia',
        'ro_qt_ros_faltantes',
        'ro_max_proc'
    ]
    COLUMN_ORDER_COMP = COLUMN_ORDER_BASE + [
        'comp_contratacao',
        'comp_item',
        'comp_id_item',
        'comp_contrato_iditem'
    ]
    COLUMN_ORDER_ISPS = COLUMN_ORDER_BASE + [
        'isps_municipio',
        'isps_indicador',
        'isps_ano_referencia'
    ]
    COLUMN_ORDER_MGP = COLUMN_ORDER_BASE + [
        'alrt_docu_dk',
        'alrt_docu_nr_mp',
        'alrt_date_referencia',
        'alrt_dias_referencia'
    ]
    COLUMN_ORDER_PPFP = COLUMN_ORDER_MGP + ['alrt_stao_dk']
    COLUMN_ORDER_GATE = COLUMN_ORDER_MGP + ['alrt_itcn_dk']
    COLUMN_ORDER_VADF = COLUMN_ORDER_MGP + ['alrt_vist_dk']
    COLUMN_ORDER_OUVI = COLUMN_ORDER_MGP + ['alrt_item_dk']

    alerta_list = {
        # 'DCTJ': [alerta_dctj],
        # 'DNTJ': [alerta_dntj],
        # 'DORD': [alerta_dord],
        'GATE': [alerta_gate, GATE_TABLE_NAME, COLUMN_ORDER_GATE],
        'BDPA': [alerta_bdpa, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'IC1A': [alerta_ic1a, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'MVVD': [alerta_mvvd, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        # 'OFFP': [alerta_offp],
        'OUVI': [alerta_ouvi, OUVI_TABLE_NAME, COLUMN_ORDER_OUVI],
        'PA1A': [alerta_pa1a, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'PPFP': [alerta_ppfp, PPFP_TABLE_NAME, COLUMN_ORDER_PPFP],
        'PRCR': [alerta_prcr, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'VADF': [alerta_vadf, VADF_TABLE_NAME, COLUMN_ORDER_VADF],
        'NF30': [alerta_nf30, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'DT2I': [alerta_dt2i, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'RO': [alerta_ro, RO_TABLE_NAME, COLUMN_ORDER_RO],
        'ABR1': [alerta_abr1, ABR1_TABLE_NAME, COLUMN_ORDER_ABR1],
        'ISPS': [alerta_isps, ISPS_TABLE_NAME, COLUMN_ORDER_ISPS],
        # 'COMP': [alerta_comp, COMP_TABLE_NAME, COLUMN_ORDER_COMP]
    }

    TABLE_NAMES = set(x[1] for x in alerta_list.values())

    def __init__(self, options):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        self.options = options
        # Setando o nome das tabelas de detalhe aqui, podemos centralizá-las como atributos de AlertaSession
        self.options['prescricao_tabela_detalhe'] = self.PRCR_DETALHE_TABLE_NAME
        self.options['isps_tabela_aux'] = self.ISPS_AUX_TABLE_NAME

        self.hist_name = lambda x: x + '_hist'

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

    def generateAlerta(self, alerta, func, table, columns):
        print('Verificando alertas do tipo: {0}'.format(alerta))
        with Timer():
            dataframe = func(self.options)
            print(dataframe)
            dataframe = dataframe.withColumn('alrt_sigla', lit(alerta).cast(StringType())) if 'alrt_sigla' not in dataframe.columns else dataframe

            # A chave DEVE ser definida dentro do alerta, senão a funcionalidade de dispensa pode não funcionar
            # formato sigla.chave.orgao
            dataframe = dataframe.withColumn('alrt_key', concat(
                col('alrt_sigla'), lit('.'),
                col('alrt_key') if 'alrt_key' in dataframe.columns else lit('KEYUNDEFINED'),
                lit('.'), col('alrt_orgi_orga_dk')
                )
            )

            dataframe.select(columns).write.mode("append").saveAsTable(self.temp_name(table))

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

                # hist_table_name = '{0}.{1}'.format(self.options['schema_alertas'], self.hist_name(table))
                # Salvar historico fazendo as verificacoes necessarias de particionamento
                # temp_table_df.withColumn("dt_partition", date_format(current_timestamp(), "yyyyMMdd"))
    
                spark.sql("drop table {0}".format(self.temp_name(table)))
