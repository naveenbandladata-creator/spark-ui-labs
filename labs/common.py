"""Shared setup for every lab. Same settings everywhere so your numbers match the video."""
import os, time
from pyspark.sql import SparkSession

def get_spark(app, **conf):
    b = (SparkSession.builder.appName(app)
         .master(os.environ.get("SPARK_MASTER", "spark://127.0.0.1:7077"))
         .config("spark.executor.memory", "512m")
         .config("spark.executor.cores", "1")
         .config("spark.cores.max", "2")
         .config("spark.driver.memory", "1g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false"))
    for k, v in conf.items():
        b = b.config(k.replace("__", "."), v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"\n>>> Spark UI for this lab: {spark.sparkContext.uiWebUrl}\n")
    return spark

def label(spark, text):
    """Name the next job(s) so they are readable in the Jobs tab."""
    sc = spark.sparkContext
    sc.setJobGroup(text, text)
    sc.setJobDescription(text)

def jobs_in(spark, text):
    return sorted(spark.sparkContext.statusTracker().getJobIdsForGroup(text))

def keep_open(spark, seconds=None):
    """The UI disappears when the app stops, so wait before stopping."""
    seconds = seconds or (float(os.environ["LAB_KEEP"]) if os.environ.get("LAB_KEEP") else None)
    ready = os.environ.get("LAB_READY_FILE")          # used by the studio capture tool
    if ready:
        open(ready, "w").write(spark.sparkContext.uiWebUrl)
    if seconds:
        time.sleep(seconds)
    else:
        try:
            input(">>> Explore the Spark UI now. Press Enter to stop the app... ")
        except EOFError:
            time.sleep(600)
    spark.stop()
