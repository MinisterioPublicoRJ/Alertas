#!/bin/sh
export PYTHONIOENCODING=utf8
spark-submit --master yarn --deploy-mode cluster \
    --keytab "/home/mpmapas/keytab/mpmapas.keytab" \
    --principal mpmapas \
    --queue root.alertas \
    --num-executors 12 \
    --driver-memory 6g \
    --executor-cores 5 \
    --executor-memory 15g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=6g \
    --conf spark.driver.memoryOverhead=2g \
    --conf spark.driver.maxResultSize=5g \
    --conf spark.default.parallelism=100 \
    --conf spark.sql.shuffle.partitions=100 \
    --conf spark.network.timeout=3600 \
    --conf spark.locality.wait=0 \
    --conf spark.shuffle.file.buffer=1024k \
    --conf spark.io.compression.lz4.blockSize=512k \
    --conf spark.maxRemoteBlockSizeFetchToMem=1500m \
    --conf spark.reducer.maxReqsInFlight=1 \
    --conf spark.shuffle.io.maxRetries=10 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --py-files src/alertas/*.py,packages/*.egg,packages/*.whl,packages/*.zip src/alertas/main.py $@

while [ $# -gt 0 ]; do

   if [[ $1 == *"-"* ]]; then
        param="${1/-/}"
        declare $param="$2"
   fi

  shift
done

impala-shell -q "INVALIDATE METADATA ${a}.mmps_alertas"
impala-shell -q "COMPUTE STATS ${a}.mmps_alertas"
