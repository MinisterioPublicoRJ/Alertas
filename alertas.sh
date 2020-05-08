#!/bin/sh
export PYTHONIOENCODING=utf8
spark2-submit --master yarn --deploy-mode cluster \
    --queue root.mpmapas \
    --num-executors 10 \
    --driver-memory 4g \
    --executor-cores 5 \
    --executor-memory 8g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.driver.maxResultSize=6000 \
    --conf spark.network.timeout=360 \
    --conf spark.speculation=true \
    --conf spark.speculation.quantile=0.5 \
    --conf spark.shuffle.io.maxRetries=5 \
    --conf spark.shuffle.io.retryWait=15s \
    --conf spark.default.parallelism=30 \
    --conf spark.sql.shuffle.partitions=30 \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M" \
    --py-files src/alertas/*.py,packages/*.egg,packages/*.whl,packages/*.zip src/alertas/main.py $@
