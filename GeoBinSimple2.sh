#!/usr/bin/env bash
hadoop fs -rm -r -skipTrash bin
spark-submit\
 --conf spark.app.id=GeoBinSimple\
 --master local[*]\
 --name GeoBin\
 --executor-cores 2\
 --num-executors 1\
 --executor-memory 512M\
 GeoBinSimple2.py
