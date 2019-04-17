#!/bin/sh
export PYTHONIOENCODING=utf8
spark2-submit --executor-memory 8g  --py-files src/shared/timer.py src/main.py 2>> error.log
