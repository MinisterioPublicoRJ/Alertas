#!/bin/sh
spark2-submit --executor-memory 8g  --py-files alertas.zip main.py
