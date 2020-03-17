#!/bin/sh
export PYTHONIOENCODING=utf8
spark2-submit --master yarn --deploy-mode cluster \
    --queue root.mpmapas \
    --num-executors 50 \
    --driver-memory 6g \
    --executor-cores 7 \
    --executor-memory 50g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=4096 \
    --conf spark.network.timeout=300 \
    --conf spark.driver.maxResultSize=6000 \
    --conf spark.default.parallelism=100 \
    --conf spark.sql.shuffle.partitions=100 \
    --py-files src/alertas/*.py,packages/*.egg,packages/*.whl,packages/*.zip src/alertas/main.py $@