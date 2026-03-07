import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"base_dir: {base_dir}")

    conf_path_file = base_dir + "/spark.conf"

    conf = conf.Config(conf_path_file)
    spark_conf = conf.spark_conf
    nc_conf = conf.nc_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"nc_conf: {nc_conf}")

    socket_df = spark \
        .readStream \
        .format("socket") \
        .option("host", nc_conf.host) \
        .option("port", nc_conf.port) \
        .load()

    log.info(f"isStreaming: {socket_df.isStreaming}")

    socket_df.printSchema()

    count_df = socket_df \
        .withColumn("word", f.explode(f.split("value", " "))) \
        .groupBy("word") \
        .agg(f.count("*").alias("count"))

    streaming_query = count_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="20 seconds") \
        .start()

    streaming_query.awaitTermination()
