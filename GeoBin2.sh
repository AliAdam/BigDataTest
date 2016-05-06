#!/usr/bin/env bash
hadoop fs -rm -r -skipTrash bin
spark-submit\
 --conf spark.app.id=GeoBin\
 --master local[*]\
 --name GeoBin\
 --executor-cores 2\
 --num-executors 1\
 --executor-memory 512M\
 GeoBin2.py
