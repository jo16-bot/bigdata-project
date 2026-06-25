#!/bin/bash

if [ -z "$VDCLOUD_USER" ]; then
    echo "Σφάλμα: source ~/bigdata-env.sh"
    exit 1
fi

AM=2121221
INPUT_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
OUTPUT_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${VDCLOUD_USER}/project2026"

spark-submit \
  --master k8s://https://10.42.0.1:6443 \
  --deploy-mode cluster \
  --name nyc-taxi-q3 \
  --py-files jobs/common.py \
  --conf spark.kubernetes.namespace=${VDCLOUD_USER}-priv \
  --conf spark.eventLog.enabled=false \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2G \
  jobs/q3.py \
  ${INPUT_BASE} \
  ${OUTPUT_BASE}
