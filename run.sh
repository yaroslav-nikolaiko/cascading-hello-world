#!/usr/bin/env bash

hdfs dfs -rm -r /tmp/wc/output
hadoop jar $1 /tmp/wc/input/input.txt /tmp/wc/output
