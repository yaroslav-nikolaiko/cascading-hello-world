#!/usr/bin/env bash

hdfs dfs -rm -r /tmp/wc/output
hadoop jar $1 /tmp/wc/input/rain.txt /tmp/wc/output
