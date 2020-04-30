#!/bin/sh
export PYTHONIOENCODING=utf8
spark2-submit --master yarn --deploy-mode cluster \
    --queue root.mpmapas \
    --num-executors 10 \
    --driver-memory 4g \
    --executor-cores 7 \
    --executor-memory 10g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=4096 \
    --conf spark.network.timeout=300 \
    --conf spark.driver.maxResultSize=6000 \
    --conf spark.default.parallelism=30 \
    --conf spark.sql.shuffle.partitions=30 \
    --py-files src/alertas/*.py,packages/*.egg,packages/*.whl,packages/*.zip src/alertas/main.py $@
