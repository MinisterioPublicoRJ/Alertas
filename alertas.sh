#!/bin/sh
export HADOOP_USER_NAME=mpmapas
export PYTHONIOENCODING=utf8
spark2-submit --master yarn --deploy-mode cluster \
    --queue root.mpmapas \
    --num-executors 10 \
    --executor-cores 1 \
    --executor-memory 8g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=4096 \
    --conf spark.network.timeout=300 \
    --py-files src/alertas/*.py,packages/*.egg,packages/*.whl src/alertas/main.py -e $SCHEMA_EXADATA -a $SCHEMA_EXADATA_AUX -i $IMPALA_HOST -o $IMPALA_PORT