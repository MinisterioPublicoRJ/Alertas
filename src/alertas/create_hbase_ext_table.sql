create external table alertas_dev.hbase_dispensados (
    disp_alrt_key string,
    disp_orgao_id int,
    disp_alerta_id string,
    disp_sigla string
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties('hbase.columns.mapping' = ':key,dados_alertas:orgao,dados_alertas:alerta_id,dados_alertas:sigla')
tblproperties('hbase.table.name' = 'dev:parquet_digital_alertas_dispensados');

create external table alertas_dev.hbase_dispensados_todos (
    disp_alrt_key string,
    disp_orgao_id int,
    disp_alerta_id string,
    disp_sigla string
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties('hbase.columns.mapping' = ':key,dados_alertas:orgao,dados_alertas:alerta_id,dados_alertas:sigla')
tblproperties('hbase.table.name' = 'dev:parquet_digital_alertas_dispensados_todos');