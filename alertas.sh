#!/bin/sh
export PYTHONIOENCODING=utf8
spark2-submit --master yarn --deploy-mode cluster \
    --queue root.alertas \
    --num-executors 12 \
    --driver-memory 6g \
    --executor-cores 5 \
    --executor-memory 10g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=6g \
    --conf spark.driver.memoryOverhead=2g \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.network.timeout=3600 \
    --conf spark.default.parallelism=30 \
    --conf spark.sql.shuffle.partitions=30 \
    --conf spark.locality.wait=0 \
    --conf spark.shuffle.io.numConnectionsPerPeer=3 \
    --conf spark.shuffle.io.maxRetries=5 \
    --conf spark.shuffle.io.retryWait=15s \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --py-files src/alertas/*.py,packages/*.egg,packages/*.whl,packages/*.zip src/alertas/main.py $@
