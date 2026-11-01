#!/bin/bash
# Stops the master and workers started by start_cluster.sh
pkill -f org.apache.spark.deploy.worker.Worker || true
pkill -f org.apache.spark.deploy.master.Master || true
echo "Cluster stopped."
