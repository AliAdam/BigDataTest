#!/usr/bin/env bash
hadoop fs -rm -r -skipTrash gps2
awk -f gps2.awk | hadoop fs -put - gps2/gps2-0001.csv
