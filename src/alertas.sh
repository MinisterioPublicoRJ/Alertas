#!/bin/sh
export HADOOP_USER_NAME=mpmapas
export PYTHONIOENCODING=utf8
spark2-submit --executor-memory 8g  --py-files alertas/*.py main.py
