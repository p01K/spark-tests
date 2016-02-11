#!/bin/bash
# MYHOST=`hostname`
MYHOST=`hostname -I | xargs`
SUBMIT="${SPARK_HOME}/bin/spark-submit"
SMASTER="spark://${MYHOST}:7077"

echo "Submitting app to $SMASTER"
TARGET='target/scala-2.10/spark-tests_2.10-1.0.jar' #change the target
PROPERTIES="${SPARK_HOME}/conf/spark-defaults.conf"
CLASS=$1
ARGS=$2
FINALARGS="$SMASTER $ARGS"
# rm -rf ../../work/*
$SUBMIT --class $CLASS --properties-file $PROPERTIES --jars $TARGET $TARGET $FINALARGS
