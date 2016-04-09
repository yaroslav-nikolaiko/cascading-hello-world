#!/usr/bin/env bash
. big-lab_init

server=${BIG_LAB_SERVER}
username=${BIG_LAB_USER}
password=${BIG_LAB_PASSWORD}
workdir=${BIG_LAB_HOME_DIR}/wordcount

jar=cascading-hello-world-1.0-SNAPSHOT.jar


#mvn clean package

sshpass -p "${password}" scp target/"${jar}" ${username}@${server}:${workdir}
sshpass -p "${password}" ssh ${username}@${server} 'bash -s' < run.sh ${workdir}/${jar}
