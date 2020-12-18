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
from alerta_febt import alerta_febt


class AlertaSession:
    alerta_list = {
        # 'DCTJ': ['Documentos criminais sem retorno do TJ a mais de 60 dias', alerta_dctj],
        # 'DNTJ': ['Documentos não criminais sem retorno do TJ a mais de 120 dias', alerta_dntj],
        # 'DORD': ['Documentos com Órgão Responsável possivelmente desatualizado', alerta_dord],
        'GATE': ['Documentos com novas ITs do GATE', alerta_gate],
        'BDPA': ['Baixas a DP em atraso', alerta_bdpa],
        'IC1A': ['ICs sem prorrogação por mais de um ano', alerta_ic1a],
        'MVVD': ['Documentos com vitimas recorrentes recebidos nos ultimos 30 dias', alerta_mvvd],
        # 'OFFP': ['Ofício fora do prazo', alerta_offp],
        'OUVI': ['Expedientes de Ouvidoria (EO) pendentes de recebimento', alerta_ouvi],
        'PA1A': ['PAs sem prorrogação por mais de um ano', alerta_pa1a],
        'PPFP': ['Procedimento Preparatório fora do prazo', alerta_ppfp],
        'PRCR': ['Processo possivelmente prescrito', alerta_prcr],
        'VADF': ['Vistas abertas em documentos já fechados', alerta_vadf],
        'NF30': ['Notícia de Fato a mais de 120 dias', alerta_nf30],
        'DT2I': ['Movimento em processo de segunda instância', alerta_dt2i],
        'RO': ['ROs não entregues pelas delegacias', alerta_ro],
        'ABR1': ['Procedimentos que têm mais de 1 ano para comunicar ao CSMP', alerta_abr1],
        'ISPS': ['Indicadores de Saneamento em Vermelho', alerta_isps],
        'FEBT': ['Mais de 30 dias sem novo RO', alerta_febt],
    }
    STATUS_RUNNING = "RUNNING"
    STATUS_FINISHED = "FINISHED"
    STATUS_ERROR = "ERROR"

    TEMP_TABLE_NAME = "temp_mmps_alertas"
    FINAL_TABLE_NAME = "mmps_alertas"
    SESSION_TABLE_NAME = "mmps_alerta_sessao"
    PRCR_DETALHE_TABLE_NAME = "mmps_alerta_detalhe_prcr"
    ISPS_AUX_TABLE_NAME = "mmps_alerta_isps_aux"

    # Ordem em que as colunas estão salvas na tabela final
    # Esta ordem deve ser mantida por conta do insertInto que é realizado
    COLUMN_ORDER = [
        'alrt_docu_dk',
        'alrt_docu_nr_mp',
        'alrt_docu_nr_externo',
        'alrt_docu_etiqueta',
        'alrt_docu_classe',
        'alrt_docu_date',
        'alrt_orgi_orga_dk',
        'alrt_classe_hierarquia',
        'alrt_dias_passados',
        'alrt_dk',
        'alrt_descricao',
        'alrt_sigla',
        'alrt_session',
        'dt_partition'
    ]

    def __init__(self, options):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        self.options = options
        # Setando o nome das tabelas de detalhe aqui, podemos centralizá-las como atributos de AlertaSession
        self.options['prescricao_tabela_detalhe'] = self.PRCR_DETALHE_TABLE_NAME
        self.options['isps_tabela_aux'] = self.ISPS_AUX_TABLE_NAME
        self.session_id = str(uuid.uuid4().int & (1<<60)-1)
        self.start_session = self.now()
        self.end_session = None
        self.status = self.STATUS_RUNNING

        # Definir o schema no nome da tabela evita possíveis conflitos
        # entre processos em produção e desenvolvimento
        self.temp_table_with_schema = '{0}.{1}'.format(
            options['schema_exadata_aux'],
            self.TEMP_TABLE_NAME
        )
        # Evita que tabela temporária de processos anteriores com erro
        # influencie no resultado do processo atual.
        spark.sql("DROP TABLE IF EXISTS {0}".format(self.temp_table_with_schema))

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
        session_df = session_df.withColumn("dt_partition", date_format(current_timestamp(), "yyyyMMdd"))
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

            # Deixar aqui por enquanto, para corrigir mais rapidamente o bug
            # Será necessária uma mudança maior de padronização mais à frente
            spark.sql("""
                SELECT D.*
                FROM documento D
                LEFT JOIN (
                    SELECT item_docu_dk
                    FROM {0}.mcpr_item_movimentacao
                    JOIN {0}.mcpr_movimentacao ON item_movi_dk = movi_dk
                    WHERE movi_orga_dk_destino IN (200819, 100500)
                ) T ON item_docu_dk = docu_dk
                LEFT JOIN (
                    SELECT vist_docu_dk, 
                        CASE
                        WHEN cod_pct IN (20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 180, 181, 182, 183)
                            AND stao_tppr_dk IN (
                                7912, 6548, 6326, 6681, 6678, 6645, 6682, 6680, 6679,
                                6644, 6668, 6666, 6665, 6669, 6667, 6664, 6655, 6662,
                                6659, 6658, 6663, 6661, 6660, 6657, 6670, 6676, 6674,
                                6673, 6677, 6675, 6672, 6018, 6341, 6338, 6019, 6017,
                                6591, 6339, 6553, 7871, 6343, 6340, 6342, 6021, 6334,
                                6331, 6022, 6020, 6593, 6332, 7872, 6336, 6333, 6335,
                                7745, 6346, 6345, 6015, 6016, 6325, 6327, 6328, 6329,
                                6330, 6337, 6344, 6656, 6671, 7869, 7870, 6324, 7834,
                                7737, 6350, 6251, 6655, 6326
                            )
                            THEN 1
                        WHEN cod_pct >= 200
                            AND stao_tppr_dk IN (
                                6682, 6669, 6018, 6341, 6338, 6019, 6017, 6591, 6339,
                                7871, 6343, 6340, 6342, 7745, 6346, 7915, 6272, 6253,
                                6392, 6377, 6378, 6359, 6362, 6361, 6436, 6524, 7737,
                                7811, 6625, 6718, 7834, 6350
                            )
                            THEN 1
                        ELSE null
                        END AS is_arquivamento
                    FROM documento
                    LEFT JOIN {1}.atualizacao_pj_pacote ON id_orgao = docu_orgi_orga_dk_responsavel
                    JOIN vista ON vist_docu_dk = docu_dk
                    JOIN {0}.mcpr_andamento ON vist_dk = pcao_vist_dk
                    JOIN {0}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
                    JOIN {0}.mcpr_tp_andamento ON tppr_dk = stao_tppr_dk
                ) A ON vist_docu_dk = docu_dk AND is_arquivamento IS NOT NULL
                WHERE item_docu_dk IS NULL
                AND vist_docu_dk IS NULL
                AND docu_fsdc_dk = 1
                AND docu_tpst_dk != 11
            """.format(self.options['schema_exadata'], self.options['schema_exadata_aux'])).createOrReplaceTempView("documentos_ativos")
            spark.catalog.cacheTable("documentos_ativos")
            spark.sql("from documentos_ativos").count()

            for alerta, (desc, func) in self.alerta_list.items():
                self.generateAlerta(alerta, desc, func)
            self.write_dataframe()
            self.wrapAlertas()

    def generateAlerta(self, alerta, desc, func):
        print('Verificando alertas do tipo: {0}'.format(alerta))
        with Timer():
            dataframe = func(self.options)
            dataframe = dataframe.withColumn("alrt_docu_dk", lit(None).cast(IntegerType())) if "alrt_docu_dk" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn("alrt_docu_nr_mp", lit(None).cast(StringType())) if "alrt_docu_nr_mp" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn("alrt_docu_nr_externo", lit(None).cast(StringType())) if "alrt_docu_nr_externo" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn("alrt_docu_etiqueta", lit(None).cast(StringType())) if "alrt_docu_etiqueta" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn("alrt_docu_classe", lit(None).cast(StringType())) if "alrt_docu_classe" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn("alrt_docu_date", lit(None).cast(TimestampType())) if "alrt_docu_date" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn("alrt_classe_hierarquia", lit(None).cast(StringType())) if "alrt_classe_hierarquia" not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_dk', lit('NO_ID')) if 'alrt_dk' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_dias_passados', lit(-1)) if 'alrt_dias_passados' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_sigla', lit(alerta).cast(StringType())) if 'alrt_sigla' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_descricao', lit(desc).cast(StringType())) if 'alrt_descricao' not in dataframe.columns else dataframe
            dataframe = dataframe.withColumn('alrt_session', lit(self.session_id).cast(StringType())).\
                withColumn("dt_partition", date_format(current_timestamp(), "yyyyMMdd"))

            dataframe.write.mode("append").saveAsTable(self.temp_table_with_schema)

    def check_table_exists(self, schema, table_name):
        spark.sql("use %s" % schema)
        result_table_check = spark.sql("SHOW TABLES LIKE '%s'" % table_name).count()
        return True if result_table_check > 0 else False

    def write_dataframe(self):
        #print('Gravando alertas do tipo {0}'.format(self.alerta_list[alerta]))
        with Timer():
            temp_table_df = spark.table(self.temp_table_with_schema)

            is_exists_table_alertas = self.check_table_exists(self.options['schema_exadata_aux'], self.FINAL_TABLE_NAME)
            table_name = '{0}.{1}'.format(self.options['schema_exadata_aux'], self.FINAL_TABLE_NAME)
            if is_exists_table_alertas:
                temp_table_df.select(self.COLUMN_ORDER).repartition(3).write.mode("overwrite").insertInto(table_name, overwrite=True)
            else:
                temp_table_df.select(self.COLUMN_ORDER).repartition(3).write.partitionBy("dt_partition").saveAsTable(table_name)
 
            spark.sql("drop table {0}".format(self.temp_table_with_schema))
