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
    ABR1_TABLE_NAME = 'mmps_alertas_abr1'
    RO_TABLE_NAME = 'mmps_alertas_ro'
    COMP_TABLE_NAME = 'mmps_alertas_comp'
    ISPS_TABLE_NAME = 'mmps_alertas_isps'
    MGP_TABLE_NAME = 'mmps_alertas_mgp'
    PPFP_TABLE_NAME = 'mmps_alertas_ppfp'
    GATE_TABLE_NAME = 'mmps_alertas_gate'
    VADF_TABLE_NAME = 'mmps_alertas_vadf'
    OUVI_TABLE_NAME = 'mmps_alertas_ouvi'

    TABLE_NAMES = [
        ABR1_TABLE_NAME,
        RO_TABLE_NAME,
        COMP_TABLE_NAME,
        ISPS_TABLE_NAME,
        MGP_TABLE_NAME,
        PPFP_TABLE_NAME,
        GATE_TABLE_NAME,
        VADF_TABLE_NAME,
        OUVI_TABLE_NAME,
    ]

    PRCR_DETALHE_TABLE_NAME = "testkey_mmps_alerta_detalhe_prcr"
    ISPS_AUX_TABLE_NAME = "testkey_mmps_alerta_isps_aux"

    # Ordem em que as colunas estão salvas na tabela final
    # Esta ordem deve ser mantida por conta do insertInto que é realizado
    COLUMN_ORDER_BASE = ['alrt_key', 'alrt_sigla', 'alrt_orgi_orga_dk']
    COLUMN_ORDER_ABR1 = COLUMN_ORDER_BASE + ['nr_procedimentos', 'ano_mes']
    COLUMN_ORDER_RO = COLUMN_ORDER_BASE + [
        'nr_delegacia',
        'qt_ros_faltantes',
        'max_proc'
    ]
    COLUMN_ORDER_COMP = COLUMN_ORDER_BASE + [
        'contratacao',
        'item',
        'id_item',
        'contrato_iditem'
    ]
    COLUMN_ORDER_ISPS = COLUMN_ORDER_BASE + [
        'municipio',
        'indicador',
        'ano_referencia'
    ]
    COLUMN_ORDER_MGP = COLUMN_ORDER_BASE + [
        'alrt_docu_dk',
        'alrt_docu_nr_mp',
        'alrt_date_referencia',
        'alrt_dias_referencia'
    ]
    COLUMN_ORDER_PPFP = COLUMN_ORDER_MGP + ['stao_dk']
    COLUMN_ORDER_GATE = COLUMN_ORDER_MGP + ['itcn_dk']
    COLUMN_ORDER_VADF = COLUMN_ORDER_MGP + ['vist_dk']
    COLUMN_ORDER_OUVI = COLUMN_ORDER_MGP + ['movi_dk']

    alerta_list = {
        # 'DCTJ': ['Documentos criminais sem retorno do TJ a mais de 60 dias', alerta_dctj],
        # 'DNTJ': ['Documentos não criminais sem retorno do TJ a mais de 120 dias', alerta_dntj],
        # 'DORD': ['Documentos com Órgão Responsável possivelmente desatualizado', alerta_dord],
        'GATE': ['Documentos com novas ITs do GATE', alerta_gate, GATE_TABLE_NAME, COLUMN_ORDER_GATE],
        'BDPA': ['Baixas a DP em atraso', alerta_bdpa, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'IC1A': ['ICs sem prorrogação por mais de um ano', alerta_ic1a, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'MVVD': ['Documentos com vitimas recorrentes recebidos nos ultimos 30 dias', alerta_mvvd, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        # 'OFFP': ['Ofício fora do prazo', alerta_offp],
        'OUVI': ['Expedientes de Ouvidoria (EO) pendentes de recebimento', alerta_ouvi, OUVI_TABLE_NAME, COLUMN_ORDER_OUVI],
        'PA1A': ['PAs sem prorrogação por mais de um ano', alerta_pa1a, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'PPFP': ['Procedimento Preparatório fora do prazo', alerta_ppfp, PPFP_TABLE_NAME, COLUMN_ORDER_PPFP],
        'PRCR': ['Processo possivelmente prescrito', alerta_prcr, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'VADF': ['Vistas abertas em documentos já fechados', alerta_vadf, VADF_TABLE_NAME, COLUMN_ORDER_VADF],
        'NF30': ['Notícia de Fato a mais de 120 dias', alerta_nf30, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'DT2I': ['Movimento em processo de segunda instância', alerta_dt2i, MGP_TABLE_NAME, COLUMN_ORDER_MGP],
        'RO': ['ROs não entregues pelas delegacias', alerta_ro, RO_TABLE_NAME, COLUMN_ORDER_RO],
        'ABR1': ['Procedimentos que têm mais de 1 ano para comunicar ao CSMP', alerta_abr1, ABR1_TABLE_NAME, COLUMN_ORDER_ABR1],
        'ISPS': ['Indicadores de Saneamento em Vermelho', alerta_isps, ISPS_TABLE_NAME, COLUMN_ORDER_ISPS],
        # 'COMP': ['Compras fora do padrão', alerta_comp, COMP_TABLE_NAME]
    }

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
        self.temp_name = lambda x: '{0}.temp_{1}'.format(options['schema_exadata_aux'], x)

        # Evita que tabela temporária de processos anteriores com erro
        # influencie no resultado do processo atual.
        for table in self.TABLE_NAMES:
            spark.sql("DROP TABLE IF EXISTS {0}".format(self.temp_name(table)))

    @staticmethod
    def now():
        return datetime.now()
    
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

            for alerta, (desc, func, table, columns) in self.alerta_list.items():
                self.generateAlerta(alerta, desc, func, table, columns)
            self.write_dataframe()

    def generateAlerta(self, alerta, desc, func, table, columns):
        print('Verificando alertas do tipo: {0}'.format(alerta))
        with Timer():
            dataframe = func(self.options)
            # dataframe = dataframe.withColumn('alrt_sigla', lit(alerta).cast(StringType())) if 'alrt_sigla' not in dataframe.columns else dataframe
            # dataframe = dataframe.withColumn('alrt_descricao', lit(desc).cast(StringType())) if 'alrt_descricao' not in dataframe.columns else dataframe

            # A chave DEVE ser definida dentro do alerta, senão a funcionalidade de dispensa pode não funcionar
            # formato sigla.chave.orgao
            dataframe = dataframe.withColumn('alrt_key', concat(
                col('alrt_sigla'),
                lit('.'),
                col('alrt_key') if 'alrt_key' in dataframe.columns else lit('KEYUNDEFINED'),
                lit('.'),
                col('alrt_orgi_orga_dk')
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
                temp_table_df = spark.table(self.temp_name(table))

                table_name = '{0}.{1}'.format(self.options['schema_exadata_aux'], table)
                temp_table_df.repartition(3).write.mode("overwrite").saveAsTable(table_name)

                # hist_table_name = '{0}.{1}'.format(self.options['schema_exadata_aux'], self.hist_name(table))
                # Salvar historico fazendo as verificacoes necessarias de particionamento
                # temp_table_df.withColumn("dt_partition", date_format(current_timestamp(), "yyyyMMdd"))
    
                spark.sql("drop table {0}".format(self.temp_name(table)))
