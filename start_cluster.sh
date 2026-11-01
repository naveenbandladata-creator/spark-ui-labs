#!/bin/bash
# Starts a small Spark standalone cluster on your machine: 1 master + 2 workers.
# Spark UI of the cluster: http://localhost:8080   Your app's UI: http://localhost:4040
# Needs: Java 17 or 21, and `pip install pyspark==4.0.1`
set -e
SPARK_HOME=${SPARK_HOME:-$(python3 -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")}
export SPARK_HOME
mkdir -p logs
nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
  --host 127.0.0.1 --port 7077 --webui-port 8080 > logs/master.log 2>&1 < /dev/null &
sleep 8
for i in 1 2; do
  nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7077 \
    --cores 2 --memory 1g --webui-port 808$i > logs/worker$i.log 2>&1 < /dev/null &
done
sleep 8
echo "Cluster starting. Open http://localhost:8080 - you should see 2 ALIVE workers."
