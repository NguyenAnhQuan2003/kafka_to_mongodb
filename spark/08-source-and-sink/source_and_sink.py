import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"base_dir: {base_dir}")

    conf_path_file = base_dir + "/spark.conf"

    spark_conf = conf.get_spark_conf(conf_path_file)

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    flight_time_df = spark.read.parquet("hdfs://namenode:8020/user/spark/data/source-and-sink/flight-time.parquet")

    flight_time_df.printSchema()

    flight_time_df.show()

    log.info(f"Num Partitions before: {flight_time_df.rdd.getNumPartitions()}")
    flight_time_df.groupBy(spark_partition_id()).count().show()

    partitioned_df = flight_time_df.repartition(5)
    log.info(f"Num Partitions after: {partitioned_df.rdd.getNumPartitions()}")
    partitioned_df.groupBy(spark_partition_id()).count().show()

    partitioned_df.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "hdfs://namenode:8020/user/spark/data/sink/avro/") \
        .save()

    flight_time_df.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("path", "hdfs://namenode:8020/user/spark/data/sink/json/") \
        .option("maxRecordsPerFile", 10000) \
        .save()

    spark.stop()
