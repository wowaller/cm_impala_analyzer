#!/bin/bash

WORK_DIR=$(dirname $0)/..

for jar in $(ls $WORK_DIR/lib/*.jar)
do
  CLASSPATH=$jar:$CLASSPATH
done

echo $CLASSPATH

java -cp $CLASSPATH -Dlog4j.configuration=file:"$WORK_DIR/conf/log4j.properties" com.cloudera.sa.cm.QueryAnalyzer $@
